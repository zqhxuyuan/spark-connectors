package org.apache.spark.sql.kafka08

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by zhengqh on 17/9/11.
  *
  * 默认不加任何配置时, 终端输入的就是消息的值. 这里模拟输入一个json字符串.并且包括普通类型和map类型
  *
  * bin/kafka-console-producer.sh --broker-list localhost:19092 --topic test
  * {"id":1,"name":"zqh1","map":{"k1":"1","k2":"1"}}
  * {"id":2,"name":"zqh2","map":{"k1":"2","k2":"2"}}
  * {"id":3,"name":"zqh3","map":{"k1":"3","k2":"3"}}
  */
object TestKafkaSchemaSource {

  // message value's custom schema
  val schemaString =  """
                        |[
                        |  {
                        |    "name": "id",
                        |    "type": "int",
                        |  },{
                        |    "name": "name",
                        |    "type": "string",
                        |  },{
                        |    "name": "map",
                        |    "type": "map<string,string>",
                        |  }
                        |]
                      """.stripMargin

  def genMessage() =
    """
      |{"id":1,"name":"zqh1","map":{"k1":"v1","k2":"v2"}}
    """.stripMargin

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", "192.168.6.52:19092")
      .option("startingoffsets", "smallest")
      .option("topics", "test")
      .option("schema", schemaString)

    val df = reader.load()

    // org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
    //df.printSchema()
    //df.show

    // DO WIH DataFrame...

    val kafka = df.writeStream
      .format("console")
      .trigger(ProcessingTime(10000L))
      .start()

    kafka.awaitTermination()
  }
}
