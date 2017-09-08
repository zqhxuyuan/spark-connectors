package com.zqh.spark.connectors.dispatcher

import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

trait Strategy[T] {
  def processor: JList[Processor[T]]

  def ref: JList[Strategy[T]]

  def compositor: JList[Compositor[T]]

  def name: String

  def initialize(name: String,
                 alg: JList[Processor[T]],
                 ref: JList[Strategy[T]],
                 com: JList[Compositor[T]],
                 params: JMap[String, Any])

  def result(params: JMap[String, Any]): JList[T]

  def configParams: JMap[String, Any]

  def stop = {}
}
