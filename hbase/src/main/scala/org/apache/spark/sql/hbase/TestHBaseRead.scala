package org.apache.spark.sql.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengqh on 17/3/7.
  */
object TestHBaseRead {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark sql hbase test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val options = Map[String, String](
      "hbase_zookeeper" -> "10.57.17.126,10.57.17.125,10.57.17.80",
      "sparksql_table_schema" -> "(key string, a string, b string, c string)",
      "hbase_table_name" -> "test",
      "hbase_table_schema" -> "(:key, cf:a, cf:b, cf:c)"
    )
    val hbasetable = sqlContext.read.format("hbase").options(options).load()

    hbasetable.printSchema()
    hbasetable.registerTempTable("test")


    sqlContext.sql("SELECT * from test limit 10").collect.foreach(println)
  }

}
