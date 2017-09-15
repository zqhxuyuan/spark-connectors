package com.zqh.spark.connectors.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Scan, Connection, Table, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hbase.bulkload.{ConfigSerDeser, RowKey, HBaseBulkLoader}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
import com.zqh.spark.connectors.hbase.HBaseTestBase._
import scala.collection.JavaConversions._

/**
  * Created by zhengqh on 17/9/14.
  *
  * Spark HBase Bulk Load 封装API
  *
  * !!!TEST NOT WORK!!!
  */
object BulkLoadTest {

  val tableName = TableName.valueOf("bulkLoad")
  val path = "/tmp/test1"
  val cf1 = "cf"

  def main(args: Array[String]) {
    HBaseTestBase.withMiniHBase{
      // Spark RDD
      val sparkConf = new SparkConf()
      //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      //sparkConf.set("spark.kryo.registrator", "org.apache.hadoop.conf.Configuration")
      //sparkConf.registerKryoClasses(Array(classOf[Configuration]))
      val sc = new SparkContext(sparkConf.setAppName("exam").setMaster("local[*]"))
      // 第一个元素是columnName, 第二个元素是columnValue
      val arr = Array(("id", "1"), ("id", "2"), ("id", "3"), ("id", "3"))
      val rdd = sc.parallelize(arr)

      @transient val conf = hbaseLocalCluster.getHbaseConfiguration
      val conn = ConnectionFactory.createConnection(conf)
      val table = createTable(conn)
      //val config = new ConfigSerDeser(conf)

      // TODO
      // java.net.ConnectException: Connection refused
      // org.apache.spark.SparkException: Task not serializable
      // Caused by: java.io.NotSerializableException: org.apache.hadoop.conf.Configuration
      // -----------------------------------------------------
      new HBaseBulkLoader(conf).bulkLoad[(String, String)](
        rdd,
        data => {
          // 随机的RowKey
          val rowKey = Bytes.toBytes(Random.nextInt())
          val cf = Bytes.toBytes(cf1)
          val qualifier = Bytes.toBytes(data._1) // columnName
          Seq((RowKey(rowKey, cf, qualifier), Bytes.toBytes(data._2))).iterator
        },
        tableName, path, cf1)
      // -----------------------------------------------------

      val load = new LoadIncrementalHFiles(conf)
      load.doBulkLoad(new Path(path), conn.getAdmin, table, conn.getRegionLocator(tableName))


      val scanner = table.getScanner(new Scan())
      try{
        for(r <- scanner){
          println("ID:" + Bytes.toString(r.getValue(cf1.getBytes, "id".getBytes)))
        }
      }finally scanner.close()
    }
  }

  def createTable(conn: Connection): Table = {
    val admin = conn.getAdmin
    val tableDescr = new HTableDescriptor(tableName)
    tableDescr.addFamily(new HColumnDescriptor(cf1.getBytes))
    println("Creating table `bulkLoad`. ")
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
    admin.createTable(tableDescr)
    println("Done!")
    conn.getTable(tableName)
  }
}
