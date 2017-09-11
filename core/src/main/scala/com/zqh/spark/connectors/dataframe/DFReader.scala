package com.zqh.spark.connectors.dataframe

import com.zqh.spark.connectors.df.SparkDFReader
import com.zqh.spark.connectors.schema.DataframeSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  */
class DFReader(config: Map[String, String]) extends SparkDFReader {

  override def readDF(spark: SparkSession): DataFrame = {
    val format = config.getOrElse("format", "")
    if(format.equals("")) return null
    var reader = spark.read.format(format)
    val json = config.getOrElse("schema", "")
    if(!json.equals("")) {
      val schema = DataframeSchema.buildSimpleSchema(json)
      reader = reader.schema(schema)
    }
    reader.options(config - format).load()
  }

}

object DFReader {
  def apply(config: Map[String, String]) = new DFReader(config)
}