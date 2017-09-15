package org.apache.spark.sql.hbase.rdd

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import unicredit.spark.hbase._

/**
  * Created by zhengqh on 17/9/15.
  */
class DefaultSource extends RelationProvider
  with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    HBaseRDDRelation(parameters)(sqlContext)
  }

  // given an dataframe, what should we do? save to somewhere!
  // df.write.format(xxx).save
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    //TODO write to hbase
    HBaseRDDRelation(parameters)(sqlContext)
  }
}

// extends TableScan to implement read from hbase
case class HBaseRDDRelation(parameters: Map[String, String])(@transient val sc: SQLContext)
  extends BaseRelation with TableScan with Serializable{

  override def sqlContext: SQLContext = sc

  // "schema" -> "key string,name string,age string"
  // key string,name string,age string => List[(key,string),(name,string),(age,string)]
  // scala.MatchError: Map(info -> Map(age -> 0, name -> zqh0)) (of class scala.collection.immutable.Map$Map1)
  override def schema: StructType = {
//    val schemaStr = parameters.getOrElse("schema", "")
//    val structList = schemaStr.split(",").map(kv => {
//      kv.split(" ")(0) -> kv.split(" ")(1) // fieldName -> fieldType
//    })
//    val structFields = structList.map(kv => {
//      val dataType = CatalystSqlParser.parseDataType(kv._2)
//      StructField(kv._1, dataType, true)
//    })
//    StructType(structFields)
    val stringType = StringType
    val columnValueType = MapType(stringType, stringType)
    val columnFamilyType = MapType(stringType, columnValueType)
    // column name is not really important
    StructType(List(StructField("key", stringType), StructField("cf", columnFamilyType)))
  }

  // schema的定义必须与RDD[Row]中的Row一致. 比如上面的schema是(id,name,age),则Row也必须是三元组
  // 如果Row是(String, Map[String, Map[String, String]]), 则schema为StringType, MapType(StringType,MapType(StringType,StringType))

  // read datasource
  lazy val buildScan: RDD[Row] = {
    val sc = sqlContext.sparkContext
    val hbaseConfig: List[(String,String)] = parameters.filter(_._1.startsWith("hbase")).map(x=>(x._1, x._2)).toList
    val zkParent = parameters.getOrElse("zookeeper.znode.parent", "/hbase")
    implicit val config = HBaseConfig((hbaseConfig ++ List(("zookeeper.znode.parent", zkParent))): _*)

    val table = parameters.getOrElse("hbase.mapreduce.inputtable", "")
    val cf = parameters.getOrElse("cfs", "").split(",").toSet
    // rdd structure: rowKey, cf, qualifier, value
    val rdd: RDD[(String, Map[String, Map[String, String]])] = sc.hbase[String](table, cf)
    rdd.map(row => Row.fromSeq(Seq(row._1, row._2)))
  }
}
