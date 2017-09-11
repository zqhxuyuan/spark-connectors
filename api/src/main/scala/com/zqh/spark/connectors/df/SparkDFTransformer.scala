package com.zqh.spark.connectors.df

import com.zqh.spark.connectors.ISparkTransformer
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkDFTransformer extends ISparkTransformer {

  def transformDF(df: DataFrame) : DataFrame

}
