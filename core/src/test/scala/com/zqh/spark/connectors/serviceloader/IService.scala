package com.zqh.spark.connectors.serviceloader

/**
  * Created by zhengqh on 17/8/31.
  */
trait IService {
  val schema: String
  def service(): Unit
}

class HDFSService extends IService {
  override def service: Unit = {
    println("HDFS Service!!!")
  }
  override val schema = "hdfs"
}

class LocalService extends IService {
  override def service = {
    println("Local Service...")
  }
  override val schema = "local"
}
