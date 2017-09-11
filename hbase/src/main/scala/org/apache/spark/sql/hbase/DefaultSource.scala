package org.apache.spark.sql.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

// 自定义数据源的入口类
class DefaultSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    // HBase的解析器, 传入参数, 返回自定义的Relation实现类
    HBaseRelation(parameters)(sqlContext)
  }
}