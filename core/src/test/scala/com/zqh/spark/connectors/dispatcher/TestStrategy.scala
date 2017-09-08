package com.zqh.spark.connectors.dispatcher
import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}
import scala.collection.JavaConversions._

class TestStrategy[T >: Boolean] extends Strategy[T]{
  var _name: String = _
  var _ref: JList[Strategy[T]] = _
  var _compositor: JList[Compositor[T]] = _
  var _processor: JList[Processor[T]] = _
  var _configParams: JMap[String, Any] = _

  def processor: JList[Processor[T]] = _processor

  def ref: JList[Strategy[T]] = _ref

  def compositor: JList[Compositor[T]] = _compositor

  def name: String = _name

  def initialize(name: String, alg: JList[Processor[T]], ref: JList[Strategy[T]], com: JList[Compositor[T]], params: JMap[String, Any]): Unit = {
    this._name = name
    this._ref = ref
    this._compositor = com
    this._processor = alg
    this._configParams = params
  }

  def result(params: JMap[String, Any]): JList[T] = {
    println("test strategy")
    for(alg <- _processor) alg.result(_configParams)
    for(com <- _compositor) com.result(_processor, _ref, null, configParams)
    new AList()
  }

  def configParams: JMap[String, Any] = _configParams
}
