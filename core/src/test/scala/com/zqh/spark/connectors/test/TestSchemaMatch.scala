package com.zqh.spark.connectors.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
  * Created by zhengqh on 17/9/18.
  */
object TestSchemaMatch {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sqlContext = spark.sqlContext

    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(List(
      (1, "A"),
      (2, "B"),
      (3, "C")
    ))
    val rddRow = rdd.map(row => Row(row._1, row._2))

    val schema = StructType(StructField("id",IntegerType) :: StructField("tag",StringType) ::Nil)
    // 转为JSON
    val df = sqlContext.applySchema(rddRow, schema).toJSON
    df.printSchema()
    df.show()

    // 保持多列
    val originDF = rdd.toDF("id", "tag")
    originDF.printSchema()

    // 创建DataFrame可以通过上面的rdd.toDF,或者通过SqlContext的createDataFrame
    val anotherDF = sqlContext.createDataFrame(rddRow, schema)
    anotherDF.printSchema()
  }
}
