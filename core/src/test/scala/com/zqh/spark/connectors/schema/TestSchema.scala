package com.zqh.spark.connectors.schema

/**
  * Created by zhengqh on 17/9/11.
  */
object TestSchema {

  def main(args: Array[String]) {
    val json =
      """
        |[{
        |  "name": "id",
        |  "type": "int"
        |},{
        |  "name": "name",
        |  "type": "string"
        |},{
        |  "name": "age",
        |  "type": "int"
        |},{
        |  "name": "sex",
        |  "type": "string"
        |},{
        |  "name": "company",
        |  "type": "string"
        |}]
      """.stripMargin

    println(json.toString.replaceAll("\n","").replaceAll(" ", ""))

    val struct = DataframeSchema.buildSimpleSchema(json)
    println(struct)
  }
}
