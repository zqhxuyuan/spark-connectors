package org.apache.spark.sql.kafka08.source

import kafka.common.TopicAndPartition
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.kafka08.util.JsonUtils
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

/**
  * An [[Offset]] for the [[KafkaSource]]. This one tracks all partitions of subscribed topics and
  * their offsets.
  */
case class KafkaSourceOffset(partitionToOffsets: Map[TopicAndPartition, LeaderOffset])
  extends Offset {
  override def toString(): String = {
    partitionToOffsets.toSeq.sortBy(_._1.toString).mkString("[", ", ", "]")
  }

  override val json = JsonUtils.partitionOffsets(partitionToOffsets)
}

/** Companion object of the [[KafkaSourceOffset]] */
object KafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicAndPartition, LeaderOffset] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => KafkaSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
    * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
    * tuples.
    */
  /*
  def apply(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case(t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }
  */

  /**
    * Returns [[KafkaSourceOffset]] from a JSON [[SerializedOffset]]
    */
  def apply(offset: SerializedOffset): KafkaSourceOffset =
    KafkaSourceOffset(JsonUtils.partitionOffsets(offset.json))
}

