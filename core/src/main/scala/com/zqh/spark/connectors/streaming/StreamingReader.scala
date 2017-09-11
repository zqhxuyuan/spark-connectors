package com.zqh.spark.connectors.streaming

import com.zqh.spark.connectors.df.SparkDFReader
import com.zqh.spark.connectors.schema.DataframeSchema
import com.zqh.spark.connectors.util.ConnectorUtils._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingReader(config: Map[String,String]) extends SparkDFReader{
  override def readDF(spark: SparkSession): DataFrame = {
    val format = config.getOrElse("format", "")
    if(format.equals("")) return null
    var reader = spark.readStream.format(format)
    val json = config.getOrElse("schema", "")
    if(!json.equals("")) {
      val schema = DataframeSchema.buildSimpleSchema(json)
      reader = reader.schema(schema)
    }
    reader.options(config - format).load()
  }
}

object StreamingReader {
  def apply(config: Map[String, String]) = new StreamingReader(config)
}