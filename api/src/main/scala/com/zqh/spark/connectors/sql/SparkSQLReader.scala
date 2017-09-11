package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.ISparkReader
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkSQLReader extends ISparkReader {

  def init(spark: SparkSession): Unit = {}

  def readSQL(spark: SparkSession) : Unit = {}

}