package org.apache.spark.sql.hbase

import org.apache.spark.sql.hbase.rdd.HBaseRDDRelation

/**
  * Created by zhengqh on 17/9/15.
  */
object TestHbaseSchema {

  def main(args: Array[String]) {
    val options = Map[String, String](
      "sql.schema" -> "(key string, name string, age string)",
      "schema" -> "(:key, info:name, info:age)"
    )

    val relation = HBaseRelation(options)(null)

    relation.hbaseTableFields.map(x=>x.fieldName + "," + x.fieldType).foreach(println)
    println("------------------------------")
    relation.fieldsRelations.foreach(println)
    println("------------------------------")
    println(relation.schema)
    println("------------------------------")

    val rddOptions = Map[String, String](
      "schema" -> "key string,info:name string,info:age string"
    )
    val relation2 = HBaseRDDRelation(rddOptions)(null)
    println(relation2.schema)
  }
}
