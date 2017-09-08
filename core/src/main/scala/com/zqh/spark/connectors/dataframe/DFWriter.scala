package com.zqh.spark.connectors.dataframe

import com.zqh.spark.connectors.SparkWriter
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/8.
  */
class DFWriter(configMap: Map[String, String]) extends SparkWriter{
  override def write(df: DataFrame): Unit = {
    val format = configMap.getOrElse("format", "")
    val mode = configMap.getOrElse("mode", "append")

    df.write.mode(mode).format(format).options(configMap).save()
  }
}

object DFWriter {
  def apply(config: Map[String, String]) = new DFWriter(config)
}