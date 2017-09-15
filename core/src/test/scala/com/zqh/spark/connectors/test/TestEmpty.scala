package com.zqh.spark.connectors.test

import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/9/15.
  */
object TestEmpty {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val df = spark.read.format("com.zqh.spark.connectors.test.empty").load()
    df.printSchema()
    df.show(1)
  }
}
