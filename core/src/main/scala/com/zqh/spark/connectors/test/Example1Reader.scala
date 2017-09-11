package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.df.SparkDFReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/11.
  */
@deprecated
class Example1Reader(config: Map[String, String]) extends SparkDFReader {
  override def readDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.sparkContext.parallelize(List(
      (1, "zhengqh", 30, "M", "td"),
      (2, "zhansan", 50, "F", "td"),
      (3, "wangwu", 33, "F", "td")
    )).toDF("id", "name", "age", "sex", "company")
  }
}
