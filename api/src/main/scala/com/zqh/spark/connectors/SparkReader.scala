package com.zqh.spark.connectors

import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkReader {

  def init(spark: SparkSession): Unit

  // SparkDFReader and SparkSQLReader can overwrite read method
  // SparkDFReader return DataFrame, SparkSQLReader return Unit
  def read(spark: SparkSession) : Any

}