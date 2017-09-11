package com.zqh.spark.connectors

import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkTransformer {

  // TODO: unify DF(DataFrame) and SQL(SparkSession)
  def transform(spark: SparkSession) : Any = {}

}
