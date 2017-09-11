package com.zqh.spark.connectors.ds

import com.zqh.spark.connectors.ISparkReader
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkDSReader[T] extends ISparkReader {

  def init(spark: SparkSession): Unit = {}

  def read(spark: SparkSession) : Dataset[T]

}