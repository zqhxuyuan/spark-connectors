package org.apache.spark.sql.hbase

import org.apache.spark.sql.SQLContext

import scala.collection.immutable.HashMap

package object hbase {

  abstract class SchemaField extends Serializable

  case class RegisteredSchemaField(fieldName: String, fieldType: String)  extends  SchemaField  with Serializable

  case class HBaseSchemaField(fieldName: String, fieldType: String)  extends  SchemaField  with Serializable

  case class Parameter(name: String)

  protected  val SPARK_SQL_TABLE_SCHEMA = Parameter("sql.schema")
  protected  val HBASE_ZOOKEEPER = Parameter("hbase.rootdir")
  protected  val HBASE_TABLE_NAME = Parameter("hbase.mapreduce.inputtable")
  protected  val HBASE_TABLE_SCHEMA = Parameter("schema")
  protected  val ROW_RANGE = Parameter("row_range")  

  /**
   * Adds a method, `hbaseTable`, to SQLContext that allows reading data stored in hbase table.
   */
  implicit class HBaseContext(sqlContext: SQLContext) {
    def hbaseTable(sparksqlTableSchema: String, hbaseZooKeeper: String, hbaseTableName: String, hbaseTableSchema: String, rowRange: String = "->") = {
      var params = new HashMap[String, String]
      params += ( SPARK_SQL_TABLE_SCHEMA.name -> hbaseZooKeeper)
      params += ( HBASE_ZOOKEEPER.name -> hbaseTableName)
      params += ( HBASE_TABLE_NAME.name -> hbaseTableName)
      params += ( HBASE_TABLE_SCHEMA.name -> hbaseTableSchema)
      //get star row and end row
      params += ( ROW_RANGE.name -> rowRange)
      sqlContext.baseRelationToDataFrame(HBaseRelation(params)(sqlContext))
    }
  }
}