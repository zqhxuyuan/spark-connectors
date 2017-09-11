package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.df.SparkDFReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/11.
  */
@deprecated
class Example2Reader(config: Map[String, String]) extends SparkDFReader {
  override def readDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.sparkContext.parallelize(List(
      ("td", "hz", "china"),
      ("tb", "hz", "china"),
      ("tx", "sz", "china")
    )).toDF("company", "city", "country")
  }
}
