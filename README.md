projectRoot="/Users/zhengqh/github/spark-connectors"
configFile="$projectRoot/core/src/main/resources/jdbc.conf"
mysql="/Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar"

bin/spark-submit --master local \
--class com.zqh.spark.connectors.ConnectorSimpleClient \
--files $configFile \
--driver-class-path $mysql --driver-java-options -Dconfig.file=$configFile \
$projectRoot/core/target/connectors-core-1.0-SNAPSHOT-jar-with-dependencies.jar spark