

# Architecture

Spark数据格式:

- DataFrame(默认无任何前缀, 或者df.)
- SQL(前缀为sql.)
- StructureStream(前缀为ss.)

# Run

```
projectRoot="/Users/zhengqh/github/spark-connectors"
configFile="$projectRoot/core/src/main/resources/dag.conf"
mysql="/Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar"

bin/spark-submit --master local \
--class com.zqh.spark.connectors.ConnectorSimpleClient \
--files $configFile --driver-java-options -Dconfig.file=$configFile \
--driver-class-path $mysql \
$projectRoot/core/target/connectors-core-jar-with-dependencies.jar spark
```

# RoadMap

- Kafka Connect/Kafka Streams/KSQL
- Flink Connectors/Flink SQL