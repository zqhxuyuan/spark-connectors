package org.apache.spark.sql.kafka08

import kafka.common.TopicAndPartition
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka08.source.{KafkaSourceOffset, KafkaSource}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.scalatest.time.SpanSugar._

/**
  * Created by zhengqh on 17/9/12.
  */
abstract class KafkaSourceTest extends StreamTest with SharedSQLContext {

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because KafkaSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffset` is latest.
    q.processAllAvailable()
    true
  }

  /**
   * Add data to Kafka.
   *
   * `topicAction` can be used to run actions for each topic before inserting data.
   */
  case class AddKafkaData(topic: String, data: Int*) extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      if (query.get.isActive) {
        // Make sure no Spark job is running when deleting a topic
        query.get.processAllAvailable()
      }

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[KafkaSource] =>
          source.asInstanceOf[KafkaSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Kafka source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Kafka source in the StreamExecution logical plan as there" +
            "are multiple Kafka sources:\n\t" + sources.mkString("\n\t"))
      }
      val kafkaSource = sources.head
      testUtils.sendMessages(topic, data.map { _.toString }.toArray)

      val Array(brokerHost, brokerPort) = testUtils.brokerAddress.split(":")
      val offset = KafkaSourceOffset(Map(TopicAndPartition(topic, 0) ->
        LeaderOffset(brokerHost, brokerPort.toInt, data.size)))
      (kafkaSource, offset)
    }

    override def toString: String =
      s"AddKafkaData(topic = $topic, data = $data)"
  }
}
