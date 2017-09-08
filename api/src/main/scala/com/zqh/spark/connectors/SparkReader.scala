package com.zqh.spark.connectors

import org.apache.spark.sql.{SparkSession, DataFrame}

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkReader {

  def init(spark: SparkSession): Unit = {}

  def read(spark: SparkSession) : DataFrame

}