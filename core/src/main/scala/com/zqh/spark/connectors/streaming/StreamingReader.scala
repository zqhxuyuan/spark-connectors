package com.zqh.spark.connectors.streaming

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingReader(config: Map[String,String]) extends SparkReader{
  override def read(spark: SparkSession): DataFrame = {
    val format = config.getOrElse("format", "")

    spark.readStream.format(format).options(config - format).load()
  }
}

object StreamingReader {
  def apply(config: Map[String, String]) = new StreamingReader(config)
}