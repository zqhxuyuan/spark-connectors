package com.zqh.spark.connectors.test.empty

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{TableScan, BaseRelation, RelationProvider}
import org.apache.spark.sql.types._

/**
  * Created by zhengqh on 17/9/15.
  */
class DefaultSource extends RelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    EmptyRelation(parameters)(sqlContext)
  }
}

case class EmptyRelation(parameters: Map[String, String])
                        (@transient val sc: SQLContext)
  extends BaseRelation with TableScan{
  override def sqlContext: SQLContext = sc

  override def schema: StructType = {
    StructType(List(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext.sparkContext.parallelize(
      List(
        (1, "A", 20),
        (2, "B", 25)
      )
    )
    rdd.map(row => Row.fromSeq(Seq(row._1, row._2, row._3)))
  }
}
