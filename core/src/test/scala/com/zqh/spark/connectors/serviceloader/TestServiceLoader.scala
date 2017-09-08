package com.zqh.spark.connectors.serviceloader

import java.util.ServiceLoader

/**
  * Created by zhengqh on 17/8/31.
  */
object TestServiceLoader {
  var serviceLoaded = false
  var cache = Map[String, IService]()

  def main(args: Array[String]) {
    loadServices()

    cache.map{case (schema, service) => {
      println(schema)
      service.service()
    }}
  }

  def loadServices(): Unit = {
    this.synchronized{
      if(!serviceLoaded) {
        val services: ServiceLoader[IService] = ServiceLoader.load(classOf[IService])
        val iterator = services.iterator()
        while(iterator.hasNext) {
          val service = iterator.next()
          cache += service.schema -> service
        }
      }
      serviceLoaded = true
    }
  }
}
