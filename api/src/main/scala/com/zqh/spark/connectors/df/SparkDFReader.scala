package com.zqh.spark.connectors.df

import com.zqh.spark.connectors.ISparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkDFReader extends ISparkReader{

  def init(spark: SparkSession): Unit = {}

  def readDF(spark: SparkSession) : DataFrame

}