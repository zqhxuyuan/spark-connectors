package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.ISparkTransformer
import org.apache.spark.sql.{SparkSession, DataFrame}

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkSQLTransformer extends ISparkTransformer {

  def transformSQL(spark: SparkSession) : Unit = {}

}
