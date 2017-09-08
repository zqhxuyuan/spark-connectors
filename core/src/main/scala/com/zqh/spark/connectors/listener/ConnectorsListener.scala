package com.zqh.spark.connectors.listener

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerApplicationEnd, SparkListener}

/**
  * Created by zhengqh on 17/9/1.
  */
class ConnectorsListener extends SparkListener with Logging{

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    log.info("spark connectors application end")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    log.info("spark connectors job end")
  }
}
