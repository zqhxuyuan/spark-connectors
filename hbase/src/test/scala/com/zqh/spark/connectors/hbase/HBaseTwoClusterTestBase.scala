package com.zqh.spark.connectors.hbase

import com.github.sakserv.minicluster.impl.{HbaseLocalCluster, ZookeeperLocalCluster}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by zhengqh on 17/9/14.
  */
object HBaseTwoClusterTestBase {

  var zookeeperLocalCluster: ZookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
    .setPort(12345)
    .setTempDir("embedded_zookeeper")
    .setZookeeperConnectionString("localhost:12345")
    .setMaxClientCnxns(60)
    .setElectionPort(20001)
    .setQuorumPort(20002)
    .setDeleteDataDirectoryOnClose(false)
    .setServerId(1)
    .setTickTime(2000)
    .build()

  var hbaseCluster1: HbaseLocalCluster = new HbaseLocalCluster.Builder()
    .setHbaseMasterPort(25111)
    .setHbaseMasterInfoPort(-1)
    .setNumRegionServers(1)
    .setHbaseRootDir("embedded_hbase1")
    .setZookeeperPort(12345)
    .setZookeeperConnectionString("localhost:12345")
    .setZookeeperZnodeParent("/hbase-unsecure1")
    .setHbaseWalReplicationEnabled(false)
    .setHbaseConfiguration(new Configuration())
    .activeRestGateway()
    .setHbaseRestHost("localhost")
    .setHbaseRestPort(28000)
    .setHbaseRestInfoPort(28001)
    .setHbaseRestReadOnly(false)
    .setHbaseRestThreadMax(100)
    .setHbaseRestThreadMin(2)
    .build()
    .build()

  var hbaseCluster2: HbaseLocalCluster = new HbaseLocalCluster.Builder()
    .setHbaseMasterPort(25112)
    .setHbaseMasterInfoPort(-1)
    .setNumRegionServers(1)
    .setHbaseRootDir("embedded_hbase2")
    .setZookeeperPort(12345)
    .setZookeeperConnectionString("localhost:12345")
    .setZookeeperZnodeParent("/hbase-unsecure2")
    .setHbaseWalReplicationEnabled(false)
    .setHbaseConfiguration(new Configuration())
    .activeRestGateway()
    .setHbaseRestHost("localhost")
    .setHbaseRestPort(28002)
    .setHbaseRestInfoPort(28003)
    .setHbaseRestReadOnly(false)
    .setHbaseRestThreadMax(100)
    .setHbaseRestThreadMin(2)
    .build()
    .build()

  def withMiniHBase(f: => Unit): Unit = {
    try {
      zookeeperLocalCluster.start()
      hbaseCluster1.start()
      hbaseCluster2.start()
      f
    }finally {
//      hbaseCluster1.stop(true)
//      hbaseCluster2.stop(true)
//      zookeeperLocalCluster.stop(true)
    }
  }

  val cfName = "info"
  val tbName = "user"

  def createTable(hbaseCluster: HbaseLocalCluster): Table = {
    val conf = hbaseCluster.getHbaseConfiguration
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val userTable = TableName.valueOf(tbName)
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor(cfName.getBytes))
    println("Creating table `user`. ")
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)
    println("Done!")
    conn.getTable(userTable)
  }

  def insertData(hbaseCluster: HbaseLocalCluster): Unit = {
    val conf = hbaseCluster.getHbaseConfiguration
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(tbName))
    for(i <- 0 until 100) {
      val p = new Put(s"id$i".getBytes)
      p.addColumn(cfName.getBytes, "name".getBytes, s"zqh$i".getBytes)
      p.addColumn(cfName.getBytes, "age".getBytes, s"$i".getBytes)
      table.put(p)
    }
    println("Done!")
    val admin = conn.getAdmin
    admin.flush(TableName.valueOf(tbName))
    table.close()
  }

  def genData() = List.range(0, 100).map(i => (i, s"name$i"))

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  val bulkLoadPath = "/Users/zhengqh/Github/spark-connectors/hbase/src/data/" +
    "embedded_hbase/data/default/user/c2e7d97b9606ff817ef8964de4cddc37"
}
