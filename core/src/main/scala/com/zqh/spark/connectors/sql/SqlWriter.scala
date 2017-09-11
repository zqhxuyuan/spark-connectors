package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.df.SparkDFWriter
import com.zqh.spark.connectors.util.ConnectorUtils._
import org.apache.spark.sql.{SparkSession, DataFrame}

/**
  * Created by zhengqh on 17/9/8.
  */
class SqlWriter(config: Map[String, String]) extends SparkSQLWriter {

  override def writeSQL(spark: SparkSession): Unit = {
    val inputTable = config.getOrElse("inputTable", "")
    val format = config.getOrElse("format", "")
    val mode = config.getOrElse("mode", "append")

    spark.sql(s"select * from $inputTable").
      write.mode(mode).format(format).options(config - format).save()
  }

}

object SqlWriter {
  def apply(config: Map[String, String]) = new SqlWriter(config)
}