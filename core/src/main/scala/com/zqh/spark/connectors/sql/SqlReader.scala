package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  */
class SqlReader(config: Map[String, String]) extends SparkReader{
  override def read(spark: SparkSession): DataFrame = {
    val sql = config.getOrElse("sql", "")
    if(sql.equals("")) return null

    spark.sql(sql)
  }
}

object SqlReader {
  def apply(config: Map[String, String]) = new SqlReader(config)
}
