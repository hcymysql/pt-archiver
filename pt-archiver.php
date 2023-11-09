<?php

###############################################
### Percona pt-archiver重构版--大表数据归档工具
### https://github.com/hcymysql/pt-archiver
###############################################

$options = getopt("h:u:p:d:P:t:w:v", ["limit:", "sleep:", "help"]);

// 检查是否传入了帮助参数，如果是，则显示使用帮助并退出
if (isset($options['help'])) {
    echo "使用方法:\n";
    echo "php pt-archiver.php -h <服务器地址> -u <用户名> -p <密码> -d <数据库名> -P <端口> -t <表名> -w <过滤条件> --limit <分批次插入数量> --sleep <插入后的休眠时间>\n";
    exit;
}

// 检查版本号参数
if (isset($options['v'])) {
    echo "更新时间：2023-11-09  版本号: 1.1.1\n";
    exit;
}

// 检查必选参数
if (!isset($options['h'], $options['u'], $options['p'], $options['d'], $options['P'], $options['t'], $options['w'], $options['limit'], $options['sleep'])) {
    die("缺少参数，请使用 --help 选项查看使用说明。\n");
}

$mysql_server = $options['h'];
$mysql_username = $options['u'];
$mysql_password = $options['p'];
$mysql_database = $options['d'];
$mysql_port = $options['P'];
$mysql_table = $options['t'];
$where_column = $options['w'];
$limit_chunk = $options['limit'];
$insert_sleep = $options['sleep'];

###############################################


######下面的代码不用更改！！！######
//########################################################//
ini_set('date.timezone','Asia/Shanghai');
header("Content-type:text/html;charset=utf-8;");
$old_c=array();
$new_c=array();

$conn=mysqli_connect($mysql_server,$mysql_username,$mysql_password,$mysql_database,$mysql_port) or die("error connecting");

if (!$conn){
	die("连接错误: " . mysqli_connect_error());
}

mysqli_query($conn,"set names 'utf8'"); 

$check_trigger_sql = "SELECT * FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '".$mysql_database."' AND TRIGGER_NAME like 'pt_archiver%';";

$query_trigger=mysqli_query($conn,$check_trigger_sql);

if(mysqli_affected_rows($conn)>0){
        die("检测到表已有触发器，退出主程序。". PHP_EOL);
}

$check_primary_key_id = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '".$mysql_database."' AND TABLE_NAME = '".$mysql_table."' AND COLUMN_KEY = 'PRI'";

$query_pri=mysqli_query($conn,$check_primary_key_id);

if(mysqli_affected_rows($conn)<=0){
        die("检测到表没有主键，退出主程序。". PHP_EOL);
}

$check_foreign_key = "SELECT TABLE_NAME,REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '${mysql_database}' AND REFERENCED_TABLE_NAME IS NOT NULL";

$query_fk=mysqli_query($conn,$check_foreign_key);

while($row_fk = mysqli_fetch_array($query_fk)) {
	if($row_fk[0] == $mysql_table) {
		echo "检测到子表含有外键，子表是：" . $row_fk[0] . "，他的父表是：".$row_fk[1]."。退出主程序。". PHP_EOL;
		exit;
	}
	if($row_fk[1] == $mysql_table) {
		echo "检测到父表含有外键，父表是：" . $row_fk[1] . "，他的子表是：".$row_fk[0]."。退出主程序。". PHP_EOL;
		exit;
	}
}

//$check_binlog_format = "SELECT VARIABLE_NAME,VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = 'BINLOG_FORMAT' AND VARIABLE_VALUE = 'ROW'";
//兼容MySQL 8.0
$check_binlog_format = "show variables where Variable_name = 'BINLOG_FORMAT' and Value = 'ROW'";

$query_binlog_format=mysqli_query($conn,$check_binlog_format);

if(mysqli_affected_rows($conn)<=0){
        die("检测到binlog_format设置的值不是ROW格式，退出主程序。". PHP_EOL);
}

######----------------------------------------------------------------------######
$sql_create_tmp = "create table IF NOT EXISTS ${mysql_table}_tmp like ${mysql_table}";
$result1 = mysqli_query($conn,$sql_create_tmp);

if ($result1) {
    echo "${mysql_table}_tmp临时表创建成功" . PHP_EOL;
} else {
    die("${mysql_table}_tmp临时表创建失败" . PHP_EOL);
}


$sql_get_column = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '".$mysql_database."' AND TABLE_NAME = '".$mysql_table."'";
$result2 = mysqli_query($conn,$sql_get_column);


while($row = mysqli_fetch_array($result2)){
	array_push($old_c,$row[0]);
	array_push($new_c,"NEW.".$row[0]);
}


$column=join(",",$old_c);
$new_column=join(",",$new_c);


$trigger= "
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
	DELETE IGNORE FROM ${mysql_database}.${mysql_table}_tmp WHERE ${mysql_database}.${mysql_table}_tmp._rowid <=> OLD._rowid;
";

echo "$trigger". PHP_EOL;

if (mysqli_multi_query($conn, $trigger)) {
    do {
	if ($result = mysqli_store_result($conn)) {
		while ($row = mysqli_fetch_row($result)) {
		}	
		mysqli_free_result($result);	
	}
	if (mysqli_more_results($conn)) {
	}
    } while (mysqli_next_result($conn));
echo "${mysql_table}表触发器创建成功" . PHP_EOL;
}
else{
     die("${mysql_table}表触发器创建失败"  .mysqli_error($conn) . PHP_EOL);
}


//抽取历史数据到临时表

$sql_get_Id = "SELECT _rowid,(SELECT max(_rowid) FROM ${mysql_database}.${mysql_table}) AS max_rowid FROM  ${mysql_database}.${mysql_table} WHERE ${where_column} order by _rowid asc LIMIT 1";
echo $sql_get_Id . PHP_EOL;

$result3 = mysqli_query($conn,$sql_get_Id);
while($row1 = mysqli_fetch_array($result3)){
        $begin_Id=number_format($row1['0'] ,0 ,'' ,''); //防止转换为科学计数法
        $max_Id=number_format($row1['1'] ,0 ,'' ,''); //防止转换为科学计数法
}
while(1==1){
$insert_select_tmp = "INSERT LOW_PRIORITY IGNORE INTO ${mysql_database}.${mysql_table}_tmp SELECT * FROM ${mysql_database}.${mysql_table} WHERE ${where_column} AND (_rowid>=".$begin_Id." AND _rowid<".($begin_Id=$begin_Id+$limit_chunk).") LOCK IN SHARE MODE ";
echo $insert_select_tmp . PHP_EOL;

mysqli_query($conn,"SET tx_isolation = 'REPEATABLE-READ'");

$result4 = mysqli_query($conn,$insert_select_tmp);

echo "". PHP_EOL;
echo "插入行数是: " . mysqli_affected_rows($conn) . PHP_EOL;

if ($result4) {
    if(mysqli_affected_rows($conn)>=1){
    	echo "${mysql_table}_tmp临时表插入成功" . PHP_EOL;
	sleep($insert_sleep);
    }
    else if($begin_Id<$max_Id){
	continue;
    }
    else{
	$exec_sql="RENAME TABLE ${mysql_table} to ${mysql_table}_bak_".date('Ymd').", ${mysql_table}_tmp to ${mysql_table};
		   DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_insert;
		   DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_update;
                   DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_delete;";
	if (mysqli_multi_query($conn, $exec_sql)) {
    	do {
		if ($result5 = mysqli_store_result($conn)) {
			while ($row2 = mysqli_fetch_row($result5)) {
		}	
		mysqli_free_result($result5);	
		}
		if (mysqli_more_results($conn)) {
		}
    	} while (mysqli_next_result($conn));
	
	echo "${mysql_table}表归档成功" . PHP_EOL;
	break;
	}	
	else{
		echo mysqli_error($conn) . PHP_EOL;
		//脚本失败的时候，触发器自动删除------------------------------ 
                $drop_trigger="DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_insert;
                               DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_update;
                               DROP TRIGGER IF EXISTS pt_archiver_${mysql_database}_${mysql_table}_delete;";
                if (mysqli_multi_query($conn, $drop_trigger)) {
                    do {
                        if ($result6 = mysqli_store_result($conn)) {
                                while ($row2 = mysqli_fetch_row($result6)) {
                                }
                                mysqli_free_result($result6);
                        }
                    } while (mysqli_next_result($conn));
                }
		//------------------------------------------------------------
     		die("${mysql_table}表归档失败"  .mysqli_error($conn) . PHP_EOL);		
	}
    }
}
else{
	die("${mysql_table}_tmp临时表插入失败 " .mysqli_error($conn). PHP_EOL);
    }
}

mysqli_close($conn);

?>
