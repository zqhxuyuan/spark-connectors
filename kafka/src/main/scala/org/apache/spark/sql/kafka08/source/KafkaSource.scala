package org.apache.spark.sql.kafka08.source

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka08.schema.{KafkaSchema, DataframeSchema}
import org.apache.spark.sql.kafka08.util.Kafka08HDFSMetadataLog
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{Broker, KafkaCluster, KafkaUtils, OffsetRange}

import scala.annotation.tailrec

/**
 * A [[Source]] that uses Kafka's SimpleConsumer API to reads data from Kafka.
 */
case class KafkaSource(
    sqlContext: SQLContext,
    topics: Set[String],
    kafkaParams: Map[String, String],
    sourceOptions: Map[String, String],
    metadataPath: String,
    startFromSmallestOffset: Boolean)
  extends Source with Logging {

  import KafkaSource._

  private val sc = sqlContext.sparkContext
  private val kc = new KafkaCluster(kafkaParams)
  private val topicPartitions = KafkaCluster.checkErrors(kc.getPartitions(topics))
  // 自定义Schema,比如value是一个json,直接转为带有Schema的DataFrame
  val transformSchema = kafkaParams.getOrElse(KafkaSchema.SCHEMA, "")

  private val maxOffsetFetchAttempts =
    sourceOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private lazy val initialPartitionOffsets = {
    val metadataLog = Kafka08HDFSMetadataLog.create(sqlContext, metadataPath, VERSION)
    metadataLog.get(0).getOrElse {
      val offsets = for {
        leaderOffsets <- (if (startFromSmallestOffset) {
          kc.getEarliestLeaderOffsets(topicPartitions)
        } else {
          kc.getLatestLeaderOffsets(topicPartitions)
        }).right
      } yield leaderOffsets

      val kafkaSourceOffset = KafkaSourceOffset(KafkaCluster.checkErrors(offsets))

      metadataLog.add(0, kafkaSourceOffset)
      log.info(s"Initial offsets: $kafkaSourceOffset")
      kafkaSourceOffset
    }.partitionToOffsets
  }

  // DefaultSource的sourceSchema方法以及这里的schema方法,都必须类型一致. 否则会报错:
  // org.apache.spark.sql.streaming.StreamingQueryException: assertion failed: Invalid batch:
  //   key#0,value#1,topic#2,partition#3,offset#4L != id#39,name#40,json#41
  override def schema: StructType = KafkaSchema.getKafkaSchema(kafkaParams)

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    val offset = KafkaSourceOffset(fetchLatestOffsets(maxOffsetFetchAttempts))
    log.debug(s"GetOffset: ${offset.partitionToOffsets.toSeq.map(_.toString).sorted}")
    Some(offset)
  }

  /**
   * Returns the data that is between the offsets
   * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
   * exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    log.info(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    val offsetRanges = fromPartitionOffsets.map { case (tp, fo) =>
      val uo = untilPartitionOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo.offset, uo.offset)
    }.toArray

    val leaders = untilPartitionOffsets.map { case (tp, lo) =>
      tp -> Broker(lo.host, lo.port)
    }

    // 将输入的Key,Value转换成Row, createRDD的最后一个类型即为Row
    // 为什么是Row,而不是Tuple()类型. 这是因为createDataFrame方法的第一参数必须是RDD[Row]
    val messageHandler =
      if(transformSchema.equals("")) {
        (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => {
          Row(mmd.key(), mmd.message(), mmd.topic, mmd.partition, mmd.offset)
        }
      } else {
        (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => Row(new String(mmd.message()))
      }

    // Create a RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = KafkaUtils.createRDD[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder,
      Row](sc, kafkaParams, offsetRanges, leaders, messageHandler)
    log.info("GetBatch generating RDD of offset range: " + offsetRanges.sortBy(_.topic).mkString(","))
    import sqlContext.sparkSession.implicits._

    // THIS IS OK, BUT BELOW WAY IS MORE READABLE
    /*
    val defaultDF = sqlContext.createDataFrame(rdd, KafkaSchema.defaultSchema)
    if(!transformSchema.equals("")) {
      val msgRDD = defaultDF.map(row => new String(row.getAs[Array[Byte]]("value")))
      sqlContext.read.schema(schema).json(msgRDD)
    } else defaultDF
    */
    // 由于messageHandler返回的是一个Row, rdd是RDD[Row],使用json时必须转为RDD[String]
    // 当然也可以把messageHandler的返回值直接转成String,这里就可以直接使用了.
    if(transformSchema.equals("")) {
      sqlContext.createDataFrame(rdd, KafkaSchema.defaultSchema)
    } else {
      sqlContext.read.schema(schema).json(rdd.map(_.getAs[String](0)))
    }
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = { }

  override def toString(): String = s"KafkaSource for topics [${topics.mkString(",")}]"

  /**
   * Fetch the latest offset of partitions.
   */
  @tailrec
  private def fetchLatestOffsets(retries: Int): Map[TopicAndPartition, LeaderOffset] = {
    val offsets = kc.getLatestLeaderOffsets(topicPartitions)
    if (offsets.isLeft) {
      val err = offsets.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        error(err)
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        fetchLatestOffsets(retries - 1)
      }
    } else {
      offsets.right.get
    }
  }
}

/** Companion object for the [[KafkaSource]]. */
object KafkaSource {
  val VERSION = 8
}

