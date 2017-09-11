package com.zqh.spark.connectors.dataframe

import com.zqh.spark.connectors.df.SparkDFTransformer
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/11.
  */
class DFTransformer(config: Map[String, String]) extends SparkDFTransformer{
  override def transformDF(df: DataFrame): DataFrame = {
    val transformer = config.getOrElse("transformer", "")
    //TODO DataFrame Transformer
    df
  }
}

object DFTransformer {
  def apply(config: Map[String, String]) = new DFTransformer(config)
}
