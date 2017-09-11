package com.zqh.spark.connectors.engine

/**
  * Created by zhengqh on 17/9/11.
  */
object TestEngine {

  def main(args: Array[String]) {
    var config = Map("format" -> "sql")
    println("format:" + SparkEngine.removePrefixFormat(config).getOrElse("format", ""))

    config = Map("format" -> "sql.")
    println("format:" + SparkEngine.removePrefixFormat(config).getOrElse("format", ""))

    config = Map("format" -> "sql.org.apache.spark.sql.hbase")
    println("format:" + SparkEngine.removePrefixFormat(config).getOrElse("format", ""))

    config = Map("format" -> "sql.jdbc")
    println("format:" + SparkEngine.removePrefixFormat(config).getOrElse("format", ""))

    config = Map("format" -> "jdbc")
    println("format:" + SparkEngine.removePrefixFormat(config).getOrElse("format", ""))

    config = Map("format" -> "org.apache.spark.sql.hbase")
    println("format:" + SparkEngine.removePrefixFormat(config).getOrElse("format", ""))
  }
}
