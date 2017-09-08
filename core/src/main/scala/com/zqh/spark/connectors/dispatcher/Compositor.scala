package com.zqh.spark.connectors.dispatcher

import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

trait Compositor[T] {

  def initialize(typeFilters: JList[String],
                 configParams: JList[JMap[String, Any]])

  def result(alg: JList[Processor[T]],
             ref: JList[Strategy[T]],
             middleResult: JList[T],
             params: JMap[String, Any]): JList[T]

  def stop = {}
}
