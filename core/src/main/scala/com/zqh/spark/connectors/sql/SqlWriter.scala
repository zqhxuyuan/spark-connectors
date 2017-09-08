package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.SparkWriter
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/8.
  */
class SqlWriter(config: Map[String, String]) extends SparkWriter{
  override def write(df: DataFrame): Unit = {
    val table = config.getOrElse("table", "")
    if(table.equals("")) return

    df.write.saveAsTable(table)
  }
}

object SqlWriter {
  def apply(config: Map[String, String]) = new SqlWriter(config)
}