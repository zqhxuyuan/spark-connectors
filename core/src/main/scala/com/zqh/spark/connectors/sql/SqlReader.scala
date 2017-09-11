package com.zqh.spark.connectors.sql

import com.zqh.spark.connectors.ISparkReader
import com.zqh.spark.connectors.dataframe.DFReader
import com.zqh.spark.connectors.df.SparkDFReader
import com.zqh.spark.connectors.schema.DataframeSchema
import com.zqh.spark.connectors.util.ConnectorUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  *
  * TODO 数据源导成Table:
  * https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html
  */
class SqlReader(config: Map[String, String]) extends SparkSQLReader {

  override def readSQL(spark: SparkSession): Unit = {
    val df = readDF(spark)
    val outputTable = config.getOrElse("outputTable", "")

    // TODO temp view = jobName + outputTableName for table conflict in production mode
    df.createOrReplaceTempView(outputTable)
  }

  // THIS CODE IS THE SAME AS DFReader
  def readDF(spark: SparkSession): DataFrame = {
    val format = config.getOrElse("format", "")
    if(format.equals("")) return null
    var reader = spark.read.format(format)
    val json = config.getOrElse("schema", "")
    if(!json.equals("")) {
      val schema = DataframeSchema.buildSimpleSchema(json)
      reader = reader.schema(schema)
    }
    reader.options(config - format).load()
  }

}

object SqlReader {
  def apply(config: Map[String, String]) = new SqlReader(config)
}
