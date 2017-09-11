package com.zqh.spark.connectors.dataframe

import com.zqh.spark.connectors.df.SparkDFTransformer
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/11.
  */
@deprecated
class SqlDFTransformer(config: Map[String, String]) extends SparkDFTransformer{

  override def transformDF(df: DataFrame): DataFrame = {
    val statement = config.getOrElse("sql", "")
    val outputTable = config.getOrElse("outputTable", "")
    if(statement.equals("") || outputTable.equals("")) return null

    val transformDF = df.sparkSession.sql(statement)
    transformDF.createOrReplaceTempView(outputTable)
    transformDF
  }

}

object SqlDFTransformer {
  def apply(config: Map[String, String]) = new SqlDFTransformer(config)
}
