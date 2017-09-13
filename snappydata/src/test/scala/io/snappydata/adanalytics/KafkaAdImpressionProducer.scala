package io.snappydata.adanalytics

import java.util.Properties

import io.snappydata.adanalytics.Configs._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import io.snappydata.adanalytics.KafkaAdImpressionProducer._

/**
  * A simple Kafka Producer program which randomly generates
  * ad impression log messages and sends it to Kafka broker.
  * This program generates and sends 10 million messages.
  */
object KafkaAdImpressionProducer{

  val props = new Properties()
  props.put("serializer.class", "io.snappydata.adanalytics.AdImpressionLogAvroEncoder")
  props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", brokerList)

  val config = new ProducerConfig(props)
  val producer = new Producer[String, AdImpressionLog](config)

  def main(args: Array[String]) {
    println("Sending Kafka messages of topic " + kafkaTopic + " to brokers " + brokerList)
    val threads = new Array[Thread](numProducerThreads)
    for (i <- 0 until numProducerThreads) {
      val thread = new Thread(new Worker())
      thread.start()
      threads(i) = thread
    }
    threads.foreach(_.join())
    println(s"Done sending $numLogsPerThread Kafka messages of topic $kafkaTopic")
    System.exit(0)
  }

  def sendToKafka(log: AdImpressionLog) = {
    producer.send(new KeyedMessage[String, AdImpressionLog](
      Configs.kafkaTopic, log.getTimestamp.toString, log))
  }
}

final class Worker extends Runnable {
  def run() {
    for (j <- 0 to numLogsPerThread by maxLogsPerSecPerThread) {
      val start = System.currentTimeMillis()
      for (i <- 1 to maxLogsPerSecPerThread) {
        sendToKafka(AdImpressionGenerator.nextNormalAdImpression())
      }
      // If one second hasn't elapsed wait for the remaining time before queueing more.
      val timeRemaining = 1000 - (System.currentTimeMillis() - start)
      if (timeRemaining > 0) {
        Thread.sleep(timeRemaining)
      }
      if (j !=0 & (j % 200000) == 0) {
        println(s" ${Thread.currentThread().getName} sent $j Kafka messages" +
          s" of topic $kafkaTopic to brokers $brokerList ")
      }
    }
  }
}
