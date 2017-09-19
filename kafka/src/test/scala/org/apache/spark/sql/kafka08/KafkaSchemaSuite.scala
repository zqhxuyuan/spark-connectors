package org.apache.spark.sql.kafka08

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.kafka08.schema.KafkaSchema

import scala.util.Random

/**
  * 测试Schema相关
  */
class KafkaSchemaSuite extends KafkaSourceTest {

  test("parsing simple json schema outside") {
    val topic = newTopic()
    testUtils.createTopic(topic)

    val implicits = spark.implicits
    import implicits._
    val sqlContext = spark.sqlContext

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingoffsets", "smallest")
      .option("topics", topic)

    val kafka = reader.load()
    val selectValue = kafka.selectExpr("CAST(value AS STRING)").as[String]

    // simple json transformation
    val json1 = selectValue.map(row => {
      val json = JSON.parse(row).asInstanceOf[java.util.Map[String, Any]]
      (json.get("id").asInstanceOf[Int], json.get("name").asInstanceOf[String])
    })
    testStream(json1)(
      makeSureGetOffsetCalled,
      AddKafkaData(topic, msg1(1), msg1(2), msg1(3)),
      CheckAnswer(answer1(1), answer1(2), answer1(3))
    )
  }

  def msg1(i: Int) =
    s"""
       |{
       |  "id": $i,
       |  "name": "name_$i"
       |}
      """.stripMargin

  def msg(i: Int) =
    s"""
       |{
       |  "id": $i,
       |  "name": "name_$i",
       |  "json": {
       |    "address": "address_$i"
       |    "city": "city_$i"
       |  }
       |}
      """.stripMargin

  def answer1(i: Int) = (i, s"name_$i")
  def answer2(i: Int) = (i, s"name_$i", Map("address" -> s"address_$i"), "city" -> s"city_$i")

  val schemaString =  """
                        |[
                        |  {
                        |    "name": "id",
                        |    "type": "int",
                        |  },{
                        |    "name": "name",
                        |    "type": "string",
                        |  },{
                        |    "name": "json",
                        |    "type": "map<string,string>",
                        |  }
                        |]
                      """.stripMargin

  val schema = KafkaSchema.getKafkaSchema(Map("schema"->schemaString))

  // TODO
  test("parsing json schema inside") {
    val topic = newTopic()
    testUtils.createTopic(topic)

    val implicits = spark.implicits
    import implicits._

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingoffsets", "smallest")
      .option("topics", topic)
      .option("schema", schemaString)

    val kafka = reader.load()
    testStream(kafka)(
      makeSureGetOffsetCalled,
      AddKafkaData(topic, msg(1), msg(2), msg(3))
      , StopStream
    )
  }

  private def newTopic(): String = s"topic-${Random.nextInt(10000)}"
}

