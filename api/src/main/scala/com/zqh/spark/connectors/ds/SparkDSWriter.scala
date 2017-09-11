package com.zqh.spark.connectors.ds

import com.zqh.spark.connectors.ISparkWriter
import org.apache.spark.sql.Dataset

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkDSWriter[T] extends ISparkWriter {

  def write(df: Dataset[T])

  def close() = {}
}
