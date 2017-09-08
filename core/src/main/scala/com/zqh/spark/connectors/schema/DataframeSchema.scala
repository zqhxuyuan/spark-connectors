package com.zqh.spark.connectors.schema

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import scala.collection.JavaConversions._

/**
  * Created by zhengqh on 17/9/8.
  */
object DataframeSchema {

  def buildSchema(json: String): StructType ={
    val structList = JSON.parse(json).
      asInstanceOf[java.util.List[java.util.Map[String, String]]]

    val structFields = structList.map(map=>{
      val dataType = CatalystSqlParser.parseDataType(map("type"))
      StructField(map("name"), dataType, true)
    })
    StructType(structFields)
  }
}
