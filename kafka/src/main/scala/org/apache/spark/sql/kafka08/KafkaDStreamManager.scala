package org.apache.spark.sql.kafka08

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * 针对kafka-0.8版本,消费者的偏移量保存在ZooKeeper中,需要手动更新和读取ZK中的偏移量
  */
class KafkaDStreamManager(val kafkaParams: Map[String, String]) {

  private val kc = new KafkaCluster(kafkaParams)
  private val groupId = kafkaParams("group.id")
  private val logger = LoggerFactory.getLogger(classOf[KafkaDStreamManager])

  // 创建数据流
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](
          ssc: StreamingContext,
          kafkaParams: Map[String, String],
          topics: Set[String]): InputDStream[(K, V)] = {

    // 在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)

    // 从zookeeper上读取offset开始消费message
    val consumerOffsets = getConsumerOffsets(topics)

    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc,
      kafkaParams, consumerOffsets, messageHandler)
  }

  private def getPartitions(topics: Set[String]) = {
    val partitionsE = kc.getPartitions(topics)
    if (partitionsE.isLeft) throw new SparkException("get kafka partition failed:")
    partitionsE.right.get
  }

  private def getConsumerOffsets(topics: Set[String]) = {
    val partitions = getPartitions(topics)

    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if (consumerOffsetsE.isLeft) throw new SparkException("get kafka consumer offsets failed:")
    val consumerOffsets = consumerOffsetsE.right.get
    consumerOffsets
  }

  //创建数据流前，根据实际消费情况更新消费offsets
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitions = getPartitions(Set(topic))

      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) { // 消费过
        /**
          * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsetsE.right.get

        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case (tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            logger.info("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (offsets.nonEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else { // 没有消费过
      val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        } else {
          leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
        }
        val offsets = leaderOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  //更新zookeeper上的消费offsets,便于监控和下次读取
  def updateZKOffsets(rdd: RDD[(Array[Byte], Array[Byte])]): Unit = {
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        logger.info(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }

}