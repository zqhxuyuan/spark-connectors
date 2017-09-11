package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.ISparkWriter
import org.apache.spark.sql.{SparkSession, DataFrame}

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkSQLWriter extends ISparkWriter {

  def writeSQL(spark: SparkSession)

  def close() = {}
}
