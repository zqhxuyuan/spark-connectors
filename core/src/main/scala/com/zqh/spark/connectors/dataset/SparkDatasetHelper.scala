package com.zqh.spark.connectors.dataset

import org.apache.spark.sql.Dataset

object SparkDatasetHelper {
  implicit class DataSetOps[I, O](ds: Dataset[I]) {
    def execute(pipeline: Dataset[I] => Dataset[O]) = ???
  }
}
