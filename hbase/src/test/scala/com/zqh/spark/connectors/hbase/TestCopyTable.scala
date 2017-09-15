package com.zqh.spark.connectors.hbase

import java.io.File
import java.nio.file.{Paths, Files, Path, StandardCopyOption}

import HBaseTwoClusterTestBase._
import com.zqh.spark.connectors.hbase.HBaseTestBase._
import com.zqh.spark.connectors.hbase.HBaseTwoClusterTestBase.bulkLoadPath
import com.zqh.spark.connectors.hbase.HBaseTwoClusterTestBase.cfName
import com.zqh.spark.connectors.hbase.HBaseTwoClusterTestBase.createTable
import com.zqh.spark.connectors.hbase.HBaseTwoClusterTestBase.insertData
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._

/**
  * Created by zhengqh on 17/9/15.
  *
  * 如果只是将hfile从一个集群直接拷贝到另一个集群,没有bulkload的话,仍然无法读出数据
  */
object TestCopyTable {

  def main(args: Array[String]): Unit = {
    testMvFile()
  }

  def testMvFile(): Unit = {
    HBaseTwoClusterTestBase.withMiniHBase{
      val cluster1 = HBaseTwoClusterTestBase.hbaseCluster1
      val cluster2 = HBaseTwoClusterTestBase.hbaseCluster2

      // insert into cluster1's table
      val table1 = createTable(cluster1)
      val table2 = createTable(cluster2)
      insertData(cluster1)

      // move hfile from cluster1 to cluster2
      val table1Folder = "/Users/zhengqh/Github/tdapp/embedded_hbase1/data/default/user"
      val table2Folder = "/Users/zhengqh/Github/tdapp/embedded_hbase2/data/default/user"
      val cf1 = new File(table1Folder).listFiles().filterNot(x=>x.isHidden)(0)
      val cf2 = new File(table2Folder).listFiles().filterNot(x=>x.isHidden)(0)
      val hfile1 = cf1.getAbsolutePath + "/info"
      val hfile2 = cf2.getAbsolutePath + "/info"
      println(hfile1)
      println(hfile2)
      while(new File(hfile1).listFiles().isEmpty) {
        Thread.sleep(5000)
      }
      copyDirFilesToDir(hfile1, hfile2)
      println("Hfiles1: " + new File(hfile1).listFiles().map(_.getAbsolutePath).mkString(","))
      println("Hfiles2: " + new File(hfile2).listFiles().map(_.getAbsolutePath).mkString(","))

      // scan cluster2's table without bulkload
      val s = new Scan()
      val scanner = table2.getScanner(s)
      try{
        for(r <- scanner){
          println("Found value: "+Bytes.toString(r.getValue(cfName.getBytes, "name".getBytes)))
        }
      }finally scanner.close()
      println("finished...")
    }
  }

  def testCopyFile(): Unit = {
    val folder1 = createOrUsePath("/tmp/test01", "folder")
    val folder2 = createOrUsePath("/tmp/test02", "folder")
    val file1 = createOrUsePath("/tmp/test01/xxx", "file")

    copyDirFilesToDir(folder1, folder2)

    val table1Folder = "/Users/zhengqh/Github/tdapp/embedded_hbase1/data/default/user"
    val table2Folder = "/Users/zhengqh/Github/tdapp/embedded_hbase2/data/default/user"

    val cf1 = new File(table1Folder).listFiles().filterNot(x=>x.isHidden)(0)
    val cf2 = new File(table2Folder).listFiles().filterNot(x=>x.isHidden)(0)
    println("CF1 NAME: " + cf1.getName)
    println("CF2 NAME: " + cf2.getName)
    val hfile1 = cf1.getAbsolutePath + "/info"
    val hfile2 = cf2.getAbsolutePath + "/info"

    println(hfile1)
    println(hfile2)
  }

  def createOrUsePath(fileName: String, fileType: String = "folder") = {
    val path = Paths.get(fileName)
    if(Files.exists(path)) path
    else {
      if(fileType.equals("file")) Files.createFile(path)
      else Files.createDirectory(path)
    }
  }

  def copyDirFilesToDir(source: Path, dest: Path) = {
    val files = source.toFile.listFiles()
    for(file <- files) {
      FileUtils.copyFileToDirectory(file, dest.toFile)
    }
  }
  def copyDirFilesToDir(sourceFolder: String, destFolder: String) = {
    val source = createOrUsePath(sourceFolder)
    val dest = createOrUsePath(destFolder)
    val files = source.toFile.listFiles()
    for(file <- files) {
      FileUtils.copyFileToDirectory(file, dest.toFile)
    }
  }
}
