package com.zqh.spark.connectors.hbase

import com.zqh.spark.connectors.hbase.HBaseTestBase._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import JavaConversions._

/**
  * Created by zhengqh on 17/9/14.
  *
  * http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/
  * https://www.iteblog.com/archives/1891.html
  * http://coolplayer.net/2016/12/26/spark-bulkload-%E5%86%99%E5%85%A5hbase/
  */
object TestSparkHBaseBulkLoad {

  def main(args: Array[String]) {
    loadHFile()
  }

  // just load aready generated hfile
  def loadHFile(): Unit = {
    HBaseTestBase.zookeeperLocalCluster.start()
    HBaseTestBase.hbaseLocalCluster.start()
    val table = createTable()

    val conf = HBaseTestBase.hbaseLocalCluster.getHbaseConfiguration
    // 已经生成的HFile, 由于不同集群生成的CF名称不同,这里把之前生成的拷贝到某个目录下

    // bulk loader
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(bulkLoadPath), new HTable(conf, tbName))

    // query table
    val scanner = table.getScanner(new Scan())
    try{
      for(r <- scanner){
        println(
          "NAME:" + Bytes.toString(r.getValue(cfName.getBytes, "name".getBytes)) +
          "AGE:" + Bytes.toString(r.getValue(cfName.getBytes, "age".getBytes))
        )
      }
    }finally scanner.close()
  }

  // save rdd as hfile, then load hfile to hbase, and query hbase
  def simpleKVLoad(): Unit = {
    HBaseTestBase.withMiniHBase {
      val table = createTable()

      val conf = HBaseTestBase.hbaseLocalCluster.getHbaseConfiguration
      val sc = new SparkContext("local", "SparkOnHBase")
      val localData = sc.parallelize(genData()).map(convertToKV(_))
      val bulkLoadPath = "/tmp/hbase/bulkload/test2"

      // write hfile use spark api
      localData.saveAsNewAPIHadoopFile(
        bulkLoadPath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat],
        conf)

      // bulk loader
      val bulkLoader = new LoadIncrementalHFiles(conf)
      bulkLoader.doBulkLoad(new Path(bulkLoadPath), new HTable(conf, tbName))

      // query table
      val scanner = table.getScanner(new Scan())
      try{
        for(r <- scanner){
          println("KEY: " + Bytes.toString(r.getRow)
            + ";NAME:" + Bytes.toString(r.getValue(cfName.getBytes, "name".getBytes)))
        }
      }finally scanner.close()
    }
  }

  def convertToKV(r: (Int, String)) = {
    val key = Bytes.toBytes(r._1)
    val cell = new KeyValue(key, Bytes.toBytes(cfName), "name".getBytes, s"${r._2}".getBytes())
    (new ImmutableBytesWritable(key), cell)
  }

}
