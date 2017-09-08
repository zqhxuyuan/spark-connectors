package com.zqh.spark.connectors

import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkWriter{

  def write(df: DataFrame)

  def close() = {}
}
