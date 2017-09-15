package org.apache.spark.sql.hbase

import java.io.Serializable

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import hbase._

/**
  *
  * val hbaseDDL = s"""
  * |CREATE TEMPORARY TABLE hbase_people
  * |USING hbase
  * |OPTIONS (
  * |   sparksql_table_schema '(row_key string, name string, age int, job string)',
  * |   hbase_table_name 'people',
  * |   hbase_table_schema '(:key , profile:name , profile:age , career:job )'
  * |)""".stripMargin
  */
case class HBaseRelation(@transient val hbaseProps: Map[String,String])
                        (@transient val sqlContext: SQLContext)
  extends BaseRelation with Serializable with TableScan {
  // 解析传入的参数
  // (:key, cf:a, cf:b, cf:c)
  val hbaseTableSchema = hbaseProps.getOrElse("schema", sys.error("not valid schema"))
  // (key string, a string, b string, c string)
  val registerTableSchema = hbaseProps.getOrElse("sql.schema", sys.error("not valid schema"))
  val rowRange = hbaseProps.getOrElse("row_range", "->")
  // get start row and end row
  val range = rowRange.split("->", -1)
  val startRowKey = range(0).trim
  val endRowKey = range(1).trim

  val tempHBaseFields = extractHBaseSchema(hbaseTableSchema) //do not use this, a temp field
  val registerTableFields = extractRegisterSchema(registerTableSchema)
  val tempFieldRelation = tableSchemaFieldMapping(tempHBaseFields,registerTableFields)

  val hbaseTableFields = feedTypes(tempFieldRelation)
  val fieldsRelations = tableSchemaFieldMapping(hbaseTableFields,registerTableFields)
  val queryColumns = getQueryTargetCloumns(hbaseTableFields)

  def feedTypes(mapping: Map[HBaseSchemaField, RegisteredSchemaField]) :  Array[HBaseSchemaField] = {
    val hbaseFields = mapping.map{
      case (k,v) =>
        val field = k.copy(fieldType=v.fieldType)
        field
    }
    hbaseFields.toArray
  }

  def isRowKey(field: HBaseSchemaField) : Boolean = {
    val cfColArray = field.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName = cfColArray(1)
    if(cfName == "" && colName == "key") true else false
  }

  //eg: f1:col1  f1:col2  f1:col3  f2:col1
  def getQueryTargetCloumns(hbaseTableFields: Array[HBaseSchemaField]): String = {
    var str = ArrayBuffer[String]()
    hbaseTableFields.foreach{ field=>
      if(!isRowKey(field)) {
        str += field.fieldName
      }
    }
    str.mkString(" ")
  }

  // 表结构
  lazy val schema = {
    val fields = hbaseTableFields.map{ field=>
      val name = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
      val relatedType = field.fieldType match  {
        case "string" =>
          SchemaType(StringType,nullable = false)
        case "int" =>
          SchemaType(IntegerType,nullable = false)
        case "long" =>
          SchemaType(LongType,nullable = false)
      }
      StructField(name,relatedType.dataType,relatedType.nullable)
    }
    StructType(fields)
  }

  def tableSchemaFieldMapping(externalHBaseTable: Array[HBaseSchemaField],
                              registerTable : Array[RegisteredSchemaField]): Map[HBaseSchemaField, RegisteredSchemaField] = {
    if(externalHBaseTable.length != registerTable.length)
      sys.error("columns size not match in definition!")
    val rs = externalHBaseTable.zip(registerTable)
    rs.toMap
  }

  /**
    * spark sql schema will be register
    *   registerTableSchema  (rowkey string, value string, column_a string)
    */
  def extractRegisterSchema(registerTableSchema: String) : Array[RegisteredSchemaField] = {
    val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    fieldsArray.map{ fildString =>
      val splitedField = fildString.split("\\s+", -1)
      RegisteredSchemaField(splitedField(0), splitedField(1))
    }
  }

  // hbase externalTableSchema (:key, f1:col1)
  def extractHBaseSchema(externalTableSchema: String) : Array[HBaseSchemaField] = {
    val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    fieldsArray.map(fildString => HBaseSchemaField(fildString,""))
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.rootdir", hbaseProps.getOrElse("hbase.rootdir","/hbase"))
    hbaseConf.set("hbase.zookeeper.quorum", hbaseProps.getOrElse("hbase.zookeeper.quorum","localhost"))
    hbaseConf.set("zookeeper.znode.parent", hbaseProps.getOrElse("zookeeper.znode.parent","/hbase"))
    hbaseConf.set("hbase.zookeeper.property.clientPort", hbaseProps.getOrElse("hbase.zookeeper.property.clientPort","2181"))

    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseProps.getOrElse("hbase.mapreduce.inputtable", ""))
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns)
    hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRowKey)
    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRowKey)

    // read hbase use map-reduce, return hbase rdd with key and value
    // the key is immutable-bytes, the value is Result
    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    val rs = hbaseRdd.map(tuple => tuple._2).map(result => {
      var values = new ArrayBuffer[Any]()
      hbaseTableFields.foreach{field=>
        values += Resolver.resolve(field,result)
      }
      Row.fromSeq(values.toSeq)
    })
    rs
  }

  private case class SchemaType(dataType: DataType, nullable: Boolean)
}