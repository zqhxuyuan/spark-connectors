package com.zqh.spark.connectors.dataframe

import com.zqh.spark.connectors.df.SparkDFWriter
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/8.
  */
class DFWriter(config: Map[String, String]) extends SparkDFWriter {

  override def writeDF(df: DataFrame): Unit = {
    val format = config.getOrElse("format", "")
    val mode = config.getOrElse("mode", "append")

    df.write.mode(mode).format(format).options(config).save()
  }

}

object DFWriter {
  def apply(config: Map[String, String]) = new DFWriter(config)
}