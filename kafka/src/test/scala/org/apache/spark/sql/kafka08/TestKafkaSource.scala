package org.apache.spark.sql.kafka08

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by zhengqh on 17/9/11.
  */
object TestKafkaSource {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    import spark.implicits

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", "192.168.6.52:19092")
      .option("startingoffsets", "smallest")
      .option("topics", "test")

    val df = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // DO WIH DataFrame...

    val kafka = df.writeStream
      .format("console")
      .trigger(ProcessingTime(10000L))
      .start()

    kafka.awaitTermination()
  }
}
