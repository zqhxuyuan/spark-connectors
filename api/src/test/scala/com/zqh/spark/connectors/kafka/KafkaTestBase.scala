package com.zqh.spark.connectors.kafka

import java.util.Properties

import com.github.sakserv.minicluster.impl.{KafkaLocalBroker, ZookeeperLocalCluster}
import junit.framework.TestCase

/**
  * Created by zhengqh on 17/9/13.
  */
class KafkaTestBase extends TestCase{

  var zookeeperLocalCluster: ZookeeperLocalCluster = null

  var kafkaLocalBroker: KafkaLocalBroker = null

  override def setUp(): Unit = {
    zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
      .setPort(12345)
      .setTempDir("/tmp")
      .setZookeeperConnectionString("localhost:12345")
      .setMaxClientCnxns(60)
      .setElectionPort(20001)
      .setQuorumPort(20002)
      .setDeleteDataDirectoryOnClose(false)
      .setServerId(1)
      .setTickTime(2000)
      .build()
    zookeeperLocalCluster.start()

    kafkaLocalBroker = new KafkaLocalBroker.Builder()
      .setKafkaHostname("localhost")
      .setKafkaPort(11111)
      .setKafkaBrokerId(0)
      .setKafkaProperties(new Properties())
      .setKafkaTempDir("/tmp")
      .setZookeeperConnectionString("localhost:12345")
      .build()
    kafkaLocalBroker.start()
  }

  override def tearDown() = {
    zookeeperLocalCluster.stop(true)
    kafkaLocalBroker.stop(true)
  }

  def testStart(): Unit = {
    println("start up zookeeper and kafka")

  }

}
