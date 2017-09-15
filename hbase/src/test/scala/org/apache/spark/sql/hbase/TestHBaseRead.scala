package org.apache.spark.sql.hbase

import com.zqh.spark.connectors.hbase.HBaseTestBase
import com.zqh.spark.connectors.hbase.HBaseTestBase._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengqh on 17/3/7.
  */
object TestHBaseRead {

  def main(args: Array[String]): Unit = {
    HBaseTestBase.withMiniHBase{
      val hbaseCluster = HBaseTestBase.hbaseLocalCluster
      insertData()

      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark sql hbase test")
      val sc = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sc)

      // TODO other parameter need to pass in
      val options = Map[String, String](
        // hbase配置项,必须与hbase的配置一样
        "hbase.zookeeper.quorum" -> hbaseCluster.getZookeeperConnectionString,
        "hbase.rootdir" -> hbaseCluster.getHbaseRootDir,
        "hbase.zookeeper.property.clientPort" -> hbaseCluster.getZookeeperPort.toString,
        "hbase.mapreduce.inputtable" -> "user",
        "zookeeper.znode.parent" -> hbaseCluster.getZookeeperZnodeParent,
        // 自定义配置项
        "sql.schema" -> "(key string, name string, age string)",
        "schema" -> "(:key, info:name, info:age)"
      )
      val hbasetable = sqlContext.read.format("org.apache.spark.sql.hbase").options(options).load()

      hbasetable.printSchema()
      hbasetable.registerTempTable("test")

      sqlContext.sql("SELECT * from test limit 10").collect.foreach(println)
    }
  }

}
