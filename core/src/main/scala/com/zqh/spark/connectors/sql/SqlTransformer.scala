package com.zqh.spark.connectors.sql

import org.apache.spark.sql.{SparkSession, DataFrame}

/**
  * Created by zhengqh on 17/9/11.
  */
class SqlTransformer(config: Map[String, String]) extends SparkSQLTransformer{

  override def transformSQL(spark: SparkSession) = {
    val statement = config.get("sql")
    val outputTable = config.get("outputTable")

    if(statement.isDefined && outputTable.isDefined) {
      spark.sql(statement.get).createOrReplaceTempView(outputTable.get)
    }
  }

}

object SqlTransformer {
  def apply(config: Map[String, String]) = new SqlTransformer(config)
}


