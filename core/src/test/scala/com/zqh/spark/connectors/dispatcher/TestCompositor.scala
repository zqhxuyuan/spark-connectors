package com.zqh.spark.connectors.dispatcher
import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

/**
  * Created by zhengqh on 17/9/4.
  */
class TestCompositor[T] extends Compositor[T]{
  private var _name: String = _
  private var _configParams: JList[JMap[String, Any]] = _

  def name(): String = _name

  override def initialize(typeFilters: JList[String], configParams: JList[JMap[String, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: JList[Processor[T]], ref: JList[Strategy[T]], middleResult: JList[T], params: JMap[String, Any]): JList[T] = {
    println("test compositor")
    new AList[T]()
  }
}
