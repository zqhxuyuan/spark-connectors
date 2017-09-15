package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.hbase.HBaseSchemaField

/**
  * Created by zhengqh on 17/9/15.
  */
object Resolver extends  Serializable {
  def resolve (hbaseField: HBaseSchemaField, result: Result ): Any = {
    val cfColArray = hbaseField.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName = cfColArray(1)
    var fieldRs: Any = null
    //resolve row key otherwise resolve column
    if(cfName=="" && colName=="key") {
      fieldRs = resolveRowKey(result, hbaseField.fieldType)
    } else {
      fieldRs = resolveColumn(result, cfName, colName,hbaseField.fieldType)
    }
    fieldRs
  }

  def resolveRowKey (result: Result, resultType: String): Any = {
    val rowkey = resultType match {
      case "string" =>
        result.getRow.map(_.toChar).mkString
      case "int" =>
        result.getRow.map(_.toChar).mkString.toInt
      case "long" =>
        result.getRow.map(_.toChar).mkString.toLong
    }
    rowkey
  }

  def resolveColumn (result: Result, columnFamily: String, columnName: String, resultType: String): Any = {
    val column = result.containsColumn(columnFamily.getBytes, columnName.getBytes) match{
      case true =>{
        resultType match {
          case "string" =>
            Bytes.toString(result.getValue(columnFamily.getBytes,columnName.getBytes))
          //result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString
          case "int" =>
            Bytes.toInt(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "long" =>
            Bytes.toLong(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "float" =>
            Bytes.toFloat(result.getValue(columnFamily.getBytes,columnName.getBytes))
          case "double" =>
            Bytes.toDouble(result.getValue(columnFamily.getBytes,columnName.getBytes))
        }
      }
      case _ => {
        resultType match {
          case "string" =>
            ""
          case "int" =>
            0
          case "long" =>
            0
        }
      }
    }
    column
  }
}
