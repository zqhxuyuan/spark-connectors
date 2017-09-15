package com.zqh.spark.connectors.hbase

import com.github.sakserv.minicluster.impl.HbaseLocalCluster
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table, Put, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import HBaseTestBase._

/**
  * Created by zhengqh on 17/9/14.
  *
  * 放在TestCase中执行,会报错序列化的异常. 用object类执行就没有问题
  */
object TestSparkReadWrite {

  def main(args: Array[String]) {
    HBaseTestBase.withMiniHBase{
      createTable()

      // write RDD to HBase
      val conf = HBaseTestBase.hbaseLocalCluster.getHbaseConfiguration
      val jobConf = new JobConf(conf, this.getClass)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, "user")

      val sc = new SparkContext("local", "SparkOnHBase")
      val rawData = List((1, "lilei", 14), (2, "hanmei", 18), (3, "someone", 38))
      val localData = sc.parallelize(rawData).map(convertToPut)
      localData.saveAsHadoopDataset(jobConf)

      // read RDD from HBase
      val scan = new Scan()
      scan.setFilter(new SingleColumnValueFilter(cfName.getBytes, "age".getBytes, CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(18)))
      conf.set(TableInputFormat.SCAN, convertScanToString(scan))
      conf.set(TableInputFormat.INPUT_TABLE, tbName)

      val usersRDD = sc.newAPIHadoopRDD(conf,
        classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], // Key
        classOf[org.apache.hadoop.hbase.client.Result]) // Value
      val count = usersRDD.count()
      println("Users RDD Count:" + count)
      usersRDD.cache()
      usersRDD.foreach { case (_, result) =>
        val key = Bytes.toInt(result.getRow)
        val name = Bytes.toString(result.getValue(cfName.getBytes, "name".getBytes))
        val age = Bytes.toInt(result.getValue(cfName.getBytes, "age".getBytes))
        println("Row key:" + key + " Name:" + name + " Age:" + age)
      }
    }
  }

  def convertToPut(triple: (Int, String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1)) // row-key
    p.addColumn(Bytes.toBytes(cfName),Bytes.toBytes("name"),Bytes.toBytes(triple._2)) // column: name
    p.addColumn(Bytes.toBytes(cfName),Bytes.toBytes("age"),Bytes.toBytes(triple._3)) // column: age
    (new ImmutableBytesWritable, p)
  }

}
