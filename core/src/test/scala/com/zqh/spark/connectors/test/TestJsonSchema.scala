package com.zqh.spark.connectors.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by zhengqh on 17/9/18.
  *
  * http://blog.antlypls.com/blog/2016/01/30/processing-json-data-with-sparksql/
  */
object TestJsonSchema {

  val event =
    """
      |{
      |  "action":"create",
      |  "timestamp":"2016-01-07T00:01:17Z",
      |  "map": {
      |    "k1": "v1",
      |    "k2": "v2"
      |  }
      |}
    """.stripMargin

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import spark.implicits._
    val eventsRDD = sc.parallelize((1, event, "2016-01-07", 0) :: Nil)

    // outer schema
    val outerSchema = StructType(
      StructField("id", IntegerType) ::
        StructField("event", StringType) ::
        StructField("timestamp", StringType) ::
        StructField("partition", IntegerType) ::
        Nil
    )

    // inner schema
    val innerSchema = StructType(
      StructField("action", StringType) ::
        StructField("timestamp", StringType) ::
        StructField("map", MapType(StringType, StringType)) :: Nil
    )

    val eventsRowRDD = eventsRDD.map(row => Row.fromTuple(row))
    val outerDF = sqlContext.createDataFrame(eventsRowRDD, outerSchema)
    outerDF.printSchema()
    outerDF.take(1).foreach(println)

    // get event field and change to rdd(second element from outer schema)
    val eventRDD = outerDF.map(row => row.getAs[String]("event"))
    val eventDF = sqlContext.read.schema(innerSchema).json(eventRDD)
    eventDF.printSchema()
    eventDF.take(1).foreach(println)
  }

  def testSimpleSchema(): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import spark.implicits._
    val eventsRDD = sc.parallelize(event :: Nil)

    // + 默认的解析方式(没有排序, 字段名称按照字母顺序: 比如action,map,timestamp)
    val df = sqlContext.read.json(eventsRDD)
    df.printSchema()
    df.show()

    // + 指定Schema有两种方式(字段顺序是固定的)
    val _schema = (new StructType)
      .add("action", StringType)
      .add("timestamp", StringType)
      .add("map", MapType(StringType, StringType))
    val df2 = sqlContext.read.schema(_schema).json(eventsRDD)
    df2.printSchema()
    df2.show()

    // + 第二种方式, 这里用::列表追加的方式.当然也可以用List()直接添加StructField
    val schema = StructType(
      StructField("action", StringType) ::
        StructField("timestamp", StringType) ::
        StructField("map", MapType(StringType, StringType)) :: Nil
    )
    val df3 = sqlContext.read.schema(schema).json(eventsRDD)
    df3.printSchema()
    df3.show()

    // ~ 只有一个字段的Schema
    val events3 = eventsRDD.map(row => Row(row))
    val df4 = sqlContext.createDataFrame(events3, StructType(StructField("json", StringType)::Nil))
    df4.printSchema() // json
    df4.show // {"action":...}

    // × ArrayIndexOutOfBoundsException: 1
    // events2只有一个字符串,尽管封装为Row, 但还是不能直接转为复杂的Schema,只能是String
    /*
    val events5 = events2.map(row => Row(row))
    val df5 = sqlContext.createDataFrame(events5, schema2)
    df5.printSchema()
    df5.show
    */

    // ~ DataFrame转为DataSet的JSON格式
    val df41 = df4.toJSON
    df41.printSchema() // value
    df41.show // {"json":{"action":...}}

    // - DataFrame要被json方法调用,只能是DataSet,或者下面的RDD,不能直接是DataFrame
    val df5 = sqlContext.read.schema(schema).json(df4.toJSON)
    df5.printSchema()
    df5.show // null

    // + DataFrame先转为RDD,然后通过schema和json方法读取RDD
    val rdd6 = df4.map(row => row.getAs[String](0))
    val df6 = sqlContext.read.schema(schema).json(rdd6)
    df6.printSchema()
    df6.show
  }
}
