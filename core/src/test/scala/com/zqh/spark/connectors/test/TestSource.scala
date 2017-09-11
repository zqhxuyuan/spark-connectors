package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.df.SparkDFReader
import com.zqh.spark.connectors.schema.DataframeSchema
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/9/11.
  */
class TestSource extends TestCase{
  val schema = """
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
  val data = """
               |[{
               |  "id": 1,
               |  "name": "zqh",
               |  "age": 11,
               |  "sex": "m",
               |  "company": "td"
               |},{
               |  "id": 2,
               |  "name": "lisi",
               |  "age": 15,
               |  "sex": "f",
               |  "company": "td"
               |}]
             """.stripMargin

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  def exampleClassNotWork(): Unit = {
    val list: List[Map[String, Any]] = DataframeSchema.buildData(data)
    val result = list.map(l => l.toList.map(_._2).toSeq.mkString(","))
    println(result.mkString("\n"))

    val map = Map(
      "class" -> "com.zqh.spark.connectors.test.Example1Reader",
      "schema" -> schema,
      "outputTable" -> "test1"
    )

    val exampleReader = new Example1Reader(map)
    println("instanceOf SparkDFReader? " + exampleReader.isInstanceOf[SparkDFReader])

    val df = spark.read.format("com.zqh.spark.connectors.test").
      schema(DataframeSchema.buildSimpleSchema(schema)).
      options(map).load()
    df.printSchema()
    df.show()
  }

  def testSchemaAndData(): Unit = {
    val map = Map(
      "schema" -> schema,
      "data" -> data,
      "outputTable" -> "test1",
      "class" -> "com.zqh.spark.connectors.test.Example1Reader"
    )

    val df = spark.read.format("com.zqh.spark.connectors.test").
      schema(DataframeSchema.buildSimpleSchema(schema)).
      options(map).load()
    df.printSchema()
    df.show()
  }

}
