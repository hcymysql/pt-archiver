Percona pt-archiver重构版--大表数据归档工具

相信很多小伙伴们，在日常对接开发时，有很多大表在业务上并没有采取任何形式的切分，数据不停地往一张表里灌入，迟早有一天，磁盘空间报警。作为一个DBA，侧重点是对数据库的操作性能（大表增加字段/索引，QPS等）和存储容量加以考虑，我们会建议开发对数据库里的大表进行数据归档处理，例如将3个月内的订单表保留在当前表，历史数据切分后保存在归档表中，之后归档表从主库上移走以便腾出磁盘空间，并将其迁移至备份机中（有条件的可以将其转换为TokuDB引擎），以便提供大数据部门抽取至HDFS上。

一张大表，我们姑且说1亿条记录，原表我要保存近7天的数据。Percona pt-archiver工具是这样做的，逐条把历史数据insert到归档表，同时删除原表数据。7天数据比如说只有10万行，那么原表会直接删除9990万行记录，操作成本太高，固需要考虑重构。

重构版是这样做的，提取你要保留的7天数据至临时表，然后老表和临时表交换名字，这样大大缩减了可用时间。

具体的工作原理：

1、如果表有触发器、或者表有外键、或者表没有主键或者主键字段默认不是id、或者binlog_format设置的值不是ROW格式，工具将直接退出，不予执行。

2、创建一个归档临时表和原表一样的空表结构。

```CREATE TABLE IF NOT EXISTS ${mysql_table}_tmp like ${mysql_table};```


3、在原表上创建增，删，改三个触发器将数据拷贝的过程中，原表产生的数据变更更新到临时表里。

```
DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_insert;

CREATE TRIGGER pt_archiver_${mysql_database}_${mysql_table}_insert AFTER INSERT
    ON ${mysql_table} FOR EACH ROW
    REPLACE INTO ${mysql_database}.${mysql_table}_tmp ($column) VALUES ($new_column);
    
DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_update;

CREATE TRIGGER pt_archiver_${mysql_database}_${mysql_table}_update AFTER UPDATE 
    ON ${mysql_table} FOR EACH ROW 
    REPLACE INTO ${mysql_database}.${mysql_table}_tmp ($column) VALUES ($new_column);
    
DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_delete;

CREATE TRIGGER pt_archiver_${mysql_database}_${mysql_table}_delete AFTER DELETE 
    ON ${mysql_table} FOR EACH ROW 
    DELETE IGNORE FROM ${mysql_database}.${mysql_table}_tmp 
    WHERE ${mysql_database}.${mysql_table}_tmp.id <=> OLD.id;
```

这三个触发器分别对应于INSERT、UPDATE、DELETE三种操作：

（1）INSERT操作，所有的INSERT INTO转换为REPLACE INTO，当有新的记录插入到原表时，如果触发器还未把该记录同步到临时表，而这条记录之前因某种原因已经存在了，那么我们就可以利用REPLACE INTO进行覆盖，这样数据也是一致的；

（2）UPDATE操作，所有的UPDATE也转换为REPLACE INTO，如果临时表不存在原表更新的该记录，那么我们就直接插入该条记录；如果该记录已经同步到临时表了，那么直接进行覆盖插入即可，所有数据与原表也是一致的；

（3）DELETE操作，原表有删除操作，会触发至临时表执行删除。如果删除的记录还未同步到临时表，那么可以不在临时表执行，因为原表中该行的数据已经被删除了，这样数据也是一致的。

4、拷贝原表数据到临时表（默认1000条一批次插入并休眠1秒）

```
INSERT LOW_PRIORITY IGNORE INTO ${mysql_database}.${mysql_table}_tmp 
SELECT * FROM ${mysql_database}.${mysql_table} WHERE id>=".$begin_Id."
 AND id<".($begin_Id=$begin_Id+$limit_chunk)." LOCK IN SHARE MODE;
```


通过主键id进行范围查找，分批次控制插入行数，已减少对原表的锁定时间（读锁/共享锁）---将大事务拆分成若干块小事务，如果临时表已经存在该记录将会忽略插入，并且在数据导入时，我们能通过sleep参数控制休眠时间，以减少对磁盘IO的冲击。

5、Rename原表为_bak，临时表Rename为原表，名字互换。

```RENAME TABLE ${mysql_table} to ${mysql_table}_bak, ${mysql_table}_tmp to ${mysql_table};```


执行表改名字，会加table metadata lock元数据表锁，但基本是瞬间结束，故对线上影响不大。

6、删除原表上的三个触发器。

```
DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_insert;

DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_update;

DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_delete;
```


至此全部过程结束，类似pt-osc原理。

注：考虑到删库跑路等安全性问题，工具没有对原表进行任何删除归档数据的操作。
--------------------------------------------------------------------------------------------------------------------------------

# 安装与使用（Centos 7系统）
```
shell> yum install php php-mysqlnd -y
```

######下面的配置信息修改成你自己的！！！######
```
$mysql_server='10.10.159.31';

$mysql_username='admin'; 

$mysql_password='123456';

$mysql_database='test';

$mysql_port='3306';

$mysql_table='sbtest1';

$where_column="update_time >= DATE_FORMAT(DATE_SUB(now(),interval 30 day),'%Y-%m-%d')";

##$where_column="id>=99900000";

$limit_chunk='10000';     ###分批次插入，默认一批插入10000行

$insert_sleep='1';        ###每次插完1000行休眠1秒
```

###############################################

执行
```
shell> php pt-archiver.php -h 192.168.0.10 -u admin -p "123456" -d test -P 3306 -t sbtest1 -w "id>=99900000" --limit 10000 --sleep 1
```
注：--limit 分批次插入，默认一批插入10000行
    --sleep 每次插完1000行休眠1秒

# 有网友反馈5.7环境有问题，请执行下面的2条语句重跑即可。 
```
mysql> set global show_compatibility_56=on; 
mysql> set global sql_mode=''; 
```

###############################################

### 如果对原表进行删除归档数据，可以借助原生工具 pt-archiver 进行分批缓慢删除。

删除数据

# pt-archiver --source h=127.0.0.1,P=3306,u=admin,p='hechunyang',D=test,t=sbtest1 --purge --charset=utf8 --where "id <= 500000" --progress=200  --limit=200 --sleep=1 --txn-size=200  --statistics

解释：删除test库，sbtest1表数据，字符集为utf8，删除条件是 id <= 5000000，每次取出200行进行处理，每处理200行则进行一次提交，每完成一次处理休眠1秒。
