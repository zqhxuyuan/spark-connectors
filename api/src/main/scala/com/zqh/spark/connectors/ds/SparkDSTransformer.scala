package com.zqh.spark.connectors.ds

import com.zqh.spark.connectors.ISparkTransformer
import org.apache.spark.sql.Dataset

/**
  * Created by zhengqh on 17/8/29.
  */
trait SparkDSTransformer[I,O] extends ISparkTransformer {

  def transform(df: Dataset[I]) : Dataset[O]

}
