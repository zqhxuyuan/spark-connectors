package com.zqh.spark.connectors.dataframe

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.schema.DataframeSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  */
class DFReader(configMap: Map[String, String]) extends SparkReader{
  override def read(spark: SparkSession): DataFrame = {
    val format = configMap.getOrElse("format", "")

    var reader = spark.read.format(format)

    val json = configMap.getOrElse("schema", "")
    if(!json.equals("")) {
      val schema = DataframeSchema.buildSchema(json)
      reader = reader.schema(schema)
    }

    reader.options(configMap - format).load()
  }

}

object DFReader {
  def apply(config: Map[String, String]) = new DFReader(config)
}