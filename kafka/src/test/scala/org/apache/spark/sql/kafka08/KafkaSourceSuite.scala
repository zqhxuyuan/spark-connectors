package org.apache.spark.sql.kafka08

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.kafka08.schema.KafkaSchema
import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import scala.util.Random
import scala.collection.JavaConversions._

class KafkaSourceSuite extends KafkaSourceTest {

  test("fetch data from Kafka stream") {
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

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(topic, "1", "2", "3"),
      CheckAnswer(2, 3, 4),
      StopStream
    )
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase.contains(m.toLowerCase))
      }
    }

    // no metadata.broker.list or bootstrap.servers specified
    testBadOptions()(
      """option 'kafka.bootstrap.servers' or 'kafka.metadata.broker.list' must be specified""")
  }

  test("unsupported kafka configs") {
    def testUnsupportedConfig(key: String, value: String = "someValue"): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka")
          .option(s"$key", value)
        reader.load()
      }
      assert(ex.getMessage.toLowerCase.contains("not supported"))
    }

    testUnsupportedConfig("kafka.group.id")
    testUnsupportedConfig("kafka.auto.offset.reset")
    testUnsupportedConfig("kafka.key.deserializer")
    testUnsupportedConfig("kafka.value.deserializer")

    testUnsupportedConfig("kafka.auto.offset.reset", "none")
    testUnsupportedConfig("kafka.auto.offset.reset", "someValue")
    testUnsupportedConfig("kafka.auto.offset.reset", "earliest")
    testUnsupportedConfig("kafka.auto.offset.reset", "latest")
  }

  private def newTopic(): String = s"topic-${Random.nextInt(10000)}"
}

