package com.zqh.spark.connectors.hdfs

import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import junit.framework.TestCase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by zhengqh on 17/9/13.
  */
class HdfsTestBase extends TestCase{

  var hdfsLocalCluster: HdfsLocalCluster = null
  var hdfs: FileSystem = null
  var config: Configuration = null

  final val HDFS_NN_PORT = 12345
  final val HDFS_WEB_PORT = 12346
  final val HOST = "localhost"
  final val HDFS_PATH = s"hdfs://localhost:$HDFS_NN_PORT"

  override def setUp(): Unit = {
    hdfsLocalCluster = new HdfsLocalCluster.Builder()
      .setHdfsNamenodePort(HDFS_NN_PORT)
      .setHdfsNamenodeHttpPort(HDFS_WEB_PORT)
      .setHdfsTempDir("/tmp")
      .setHdfsNumDatanodes(1)
      .setHdfsEnablePermissions(false)
      .setHdfsFormat(true)
      .setHdfsEnableRunningUserAsProxyUser(true)
      .setHdfsConfig(new Configuration())
      .build()
    hdfsLocalCluster.start()
    hdfs = hdfsLocalCluster.getHdfsFileSystemHandle
    config = hdfsLocalCluster.getHdfsConfig

    config.set("custormStrKey", "custormStrValue")
    config.setInt("custormIntKey", 1)
  }

  override def tearDown(): Unit = {
    hdfs.close()
    hdfsLocalCluster.stop(true)
  }

  def testBasicHdfs(): Unit = {
    assert(hdfs.getScheme.equalsIgnoreCase("hdfs"))

    assert(hdfs.getUri.toString.equals(HDFS_PATH))
    assert(config.getStrings("fs.defaultFS").mkString(",").equals(HDFS_PATH))

    assert(config.getInt("custormIntKey", 0) == 1)
  }

}
