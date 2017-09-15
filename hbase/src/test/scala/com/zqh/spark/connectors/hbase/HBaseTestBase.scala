package com.zqh.spark.connectors.hbase

import com.github.sakserv.minicluster.impl.{ZookeeperLocalCluster, HbaseLocalCluster}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._

import scala.collection.mutable

/**
  * Created by zhengqh on 17/9/14.
  */
object HBaseTestBase {

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

  var hbaseLocalCluster: HbaseLocalCluster = new HbaseLocalCluster.Builder()
    .setHbaseMasterPort(25111)
    .setHbaseMasterInfoPort(-1)
    .setNumRegionServers(1)
    .setHbaseRootDir("embedded_hbase")
    .setZookeeperPort(12345)
    .setZookeeperConnectionString("localhost:12345")
    .setZookeeperZnodeParent("/hbase-unsecure")
    .setHbaseWalReplicationEnabled(false)
    .setHbaseConfiguration(new Configuration())
    .activeRestGateway()
    .setHbaseRestHost("localhost")
    .setHbaseRestPort(28000)
    .setHbaseRestReadOnly(false)
    .setHbaseRestThreadMax(100)
    .setHbaseRestThreadMin(2)
    .build()
    .build()

  def withMiniHBase(f: => Unit): Unit = {
    try {
      zookeeperLocalCluster.start()
      hbaseLocalCluster.start()
      f
    }finally {
      hbaseLocalCluster.stop(true)
      zookeeperLocalCluster.stop(true)
    }
  }

  val cfName = "info"
  val tbName = "user"

  def createTable(): Table = {
    val conf = hbaseLocalCluster.getHbaseConfiguration
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

  def insertData(): Unit = {
    val table = createTable()
    for(i <- 0 until 100) {
      val p = new Put(s"id$i".getBytes)
      p.addColumn(cfName.getBytes, "name".getBytes, s"zqh$i".getBytes)
      p.addColumn(cfName.getBytes, "age".getBytes, s"$i".getBytes)
      table.put(p)
    }
  }

  def genData() = List.range(0, 100).map(i => (i, s"name$i"))

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  val bulkLoadPath = "/Users/zhengqh/Github/spark-connectors/hbase/src/data/" +
    "embedded_hbase/data/default/user/c2e7d97b9606ff817ef8964de4cddc37"
}
