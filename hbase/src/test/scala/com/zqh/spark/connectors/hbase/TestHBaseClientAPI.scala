package com.zqh.spark.connectors.hbase

import junit.framework.TestCase
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes}
import scala.collection.JavaConversions._
import HBaseTestBase._

/**
  * Created by zhengqh on 17/9/14.
  */
class TestHBaseClientAPI extends TestCase {

  // basic hbase usage, start and stop hbase
  def testHBaseAPI() = {
    HBaseTestBase.withMiniHBase{
      val table = createTable()
      val p = new Put("id001".getBytes)
      p.addColumn(cfName.getBytes, "name".getBytes, "wuchong".getBytes)
      table.put(p)

      val g = new Get("id001".getBytes)
      val result = table.get(g)
      val value = Bytes.toString(result.getValue(cfName.getBytes,"name".getBytes))
      println("GET id001 :"+value)

      val s = new Scan()
      s.addColumn(cfName.getBytes,"name".getBytes)
      val scanner = table.getScanner(s)
      try{
        for(r <- scanner){
          println("Found row: "+r)
          println("Found value: "+Bytes.toString(r.getValue(cfName.getBytes,"name".getBytes)))
        }
      }finally scanner.close()

      val d = new Delete("id001".getBytes)
      d.addColumn(cfName.getBytes,"name".getBytes)
      table.delete(d)
    }
  }

  // Just Generate HBase Data, and see what's inside
  def testHBase(): Unit = {
    HBaseTestBase.zookeeperLocalCluster.start()
    HBaseTestBase.hbaseLocalCluster.start()
    createTable()
    insertData()
  }
}
