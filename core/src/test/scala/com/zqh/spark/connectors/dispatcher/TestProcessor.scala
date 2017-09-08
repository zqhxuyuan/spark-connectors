package com.zqh.spark.connectors.dispatcher
import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

class TestProcessor[T]  extends Processor[T]{
  private var _name: String = _
  private var _configParams: JList[JMap[String, Any]] = _

  def name(): String = _name

  def initialize(name: String, params: JList[JMap[String, Any]]): Unit = {
    this._name = name
    this._configParams = params
  }

  def result(params: JMap[String, Any]): JList[T] = {
    println("test processor")
    new AList[T]()
  }
}
