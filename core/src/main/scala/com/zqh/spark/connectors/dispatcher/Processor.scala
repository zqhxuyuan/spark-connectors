package com.zqh.spark.connectors.dispatcher

import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

trait Processor[T] {
  def initialize(name: String, params: JList[JMap[String, Any]])
  def result(params: JMap[String, Any]): JList[T]
  def name(): String
  def stop = {}
}
