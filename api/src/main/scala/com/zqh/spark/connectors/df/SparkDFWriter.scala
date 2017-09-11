package com.zqh.spark.connectors.df

import com.zqh.spark.connectors.ISparkWriter
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkDFWriter extends ISparkWriter {

  def writeDF(df: DataFrame)

  def close() = {}
}
