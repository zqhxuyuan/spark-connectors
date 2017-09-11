package com.zqh.spark.connectors

import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkWriter{

  // TODO: unify DF and SQL
  def write(spark: SparkSession)

  def close() = {}
}
