package com.zqh.spark.connectors

import org.apache.spark.sql._

/**
  * Created by zhengqh on 17/9/11.
  */
trait ISparkReader {

  def init(spark: SparkSession): Unit = {}

  def read(spark: SparkSession) : Any
}

class ISparkDFReader extends ISparkReader {
  override def init(spark: SparkSession): Unit = {
    println("init spark df reader")
  }

  override def read(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    spark.sparkContext.parallelize(List(1,2,3)).toDF()
  }
}

class ISparkSqlReader extends ISparkReader {
  override def init(spark: SparkSession): Unit = {
    println("init spark sql reader")
  }

  override def read(spark: SparkSession) : Unit = {
    import spark.implicits._
    spark.sparkContext.parallelize(List(1,2,3)).toDF().createOrReplaceTempView("test")
  }
}

object TestUnifySparkReaderOp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val reader1 = new ISparkDFReader
    val df1 = reader1.read(spark)
    df1.show

    println("-----------------------")

    val reader2 = new ISparkSqlReader
    reader2.read(spark)
    spark.sql("select * from test").show
  }
}