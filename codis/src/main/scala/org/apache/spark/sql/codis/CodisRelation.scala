package org.apache.spark.sql.codis

import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by zhengqh on 17/9/7.
  */
case class CodisRelation(scheme: StructType)
                        (@transient val sparkSession: SparkSession)
  extends BaseRelation with Serializable {

  override def sqlContext: SQLContext = {
    sparkSession.sqlContext
  }

  override def schema: StructType = {
    scheme
  }
}
