package com.zqh.spark.connectors.hdfs

import com.github.sakserv.minicluster.impl.YarnLocalCluster
import junit.framework.TestCase
import org.apache.hadoop.conf.Configuration

/**
  * Created by zhengqh on 17/9/13.
  */
class YarnTestBase extends TestCase{

  var yarnLocalCluster: YarnLocalCluster = null
  final val YARN_RM_PORT = 37001
  final val YARN_WEB_PORT = 37004
  final val HOST = "localhost"

  override def setUp(): Unit = {
    yarnLocalCluster = new YarnLocalCluster.Builder()
      .setNumNodeManagers(1)
      .setNumLocalDirs(1)
      .setNumLogDirs(1)
      .setResourceManagerAddress(s"$HOST:$YARN_RM_PORT")
      .setResourceManagerHostname(HOST)
      .setResourceManagerSchedulerAddress(s"$HOST:37002")
      .setResourceManagerResourceTrackerAddress(s"$HOST:37003")
      .setResourceManagerWebappAddress(s"$HOST:$YARN_WEB_PORT")
      .setUseInJvmContainerExecutor(false)
      .setConfig(new Configuration())
      .build()
    yarnLocalCluster.start()
  }

  override def tearDown(): Unit = {
    yarnLocalCluster.stop(true)
  }

  // TODO library dependency problem
  // java.lang.NoSuchMethodError: org.apache.hadoop.yarn.server.utils.BuilderUtils.newApplicationResourceUsageReport
  // (IILorg/apache/hadoop/yarn/api/records/Resource;Lorg/apache/hadoop/yarn/api/records/Resource;
  // Lorg/apache/hadoop/yarn/api/records/Resource;JJ)Lorg/apache/hadoop/yarn/api/records/ApplicationResourceUsageReport;
  def testYarnBasic(): Unit = {
    println("start yarn...")
  }
}
