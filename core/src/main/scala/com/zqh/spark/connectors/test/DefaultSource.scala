package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.df.SparkDFReader
import com.zqh.spark.connectors.schema.DataframeSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
  * Created by zhengqh on 17/9/11.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ExampleRelation(parameters)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext,
                              saveMode:   SaveMode,
                              parameters: Map[String, String],
                              dataframe:  DataFrame): BaseRelation = {
    dataframe.printSchema()
    dataframe.show(100)
    ExampleRelation(parameters)(sqlContext)
  }
}

case class ExampleRelation(@transient val parameters: Map[String,String])
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation with Serializable with TableScan {

  override def schema: StructType = {
    val json = parameters.getOrElse("schema", "")
    var schema: StructType = null
    if(!json.equals("")) {
      schema = DataframeSchema.buildSimpleSchema(json)
      println(schema)
    }
    schema
  }

  // build rdd from map data, notice keep map order
  override def buildScan(): RDD[Row] = {
    var rdd: RDD[Row] = null
    //TODO NOT WORK THIS WAY
    val dfName = parameters.getOrElse("class", "")
    if(!dfName.equals("")) {
      val dfClass = Class.forName(dfName)
      if(dfClass.isInstanceOf[SparkDFReader]) {
        println("dfClass: " + dfClass)
        val dfReader = dfClass.getConstructor(classOf[Map[String, String]]).
          newInstance(parameters).asInstanceOf[SparkDFReader]
        println("实例化读取器: " + dfReader)
        rdd = dfReader.readDF(sqlContext.sparkSession).rdd
      }
    }

    // demo data. 确保Row中的顺序与Schema一致
//    sqlContext.sparkContext.parallelize(Seq(
//      Row(1, "zqh", 11, "m", "td"),
//      Row(2, "lisi", 22, "f", "td")
//    ))

    val data = parameters.getOrElse("data", "")
    val json = parameters.getOrElse("schema", "")
    val names = DataframeSchema.getSchemaNames(json)
    //确保Row中的顺序与Schema一致
    if(!data.equals("")) {
      val list: List[Map[String, Any]] = DataframeSchema.buildData(data)
      val result = sqlContext.sparkContext.parallelize(list)
      val rows = result.map(ele => {
        val seq = names.map(name => ele(name)).toSeq
        Row.fromSeq(seq)
      })
      rdd = rows
    }
    rdd
  }
}

