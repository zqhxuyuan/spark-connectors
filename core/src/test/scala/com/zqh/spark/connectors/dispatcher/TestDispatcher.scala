package com.zqh.spark.connectors.dispatcher

import com.zqh.spark.connectors.config.ConfigUtils
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}
import scala.collection.JavaConversions._

/**
  * Created by zhengqh on 17/9/4.
  */
class TestDispatcher extends FlatSpec with Matchers with BeforeAndAfter{

  var configStr: String = _
  var mapping: DefaultShortNameMapping = _

  before {
    mapping = new DefaultShortNameMapping()
    configStr = ConfigUtils.loadConfigFile2String("strategy.v2.json")
  }

  "dispatcher by json" should "dispatch example" in {
    val dispatcher = StrategyDispatcher.getOrCreate(configStr, mapping)
    println(dispatcher._config)

    val strategies = dispatcher.strategies
    strategies.map{
      case (k,v) =>
        println(k)
        dispatcher.dispatch(k)
    }
  }

  "dispatcher by config" should "work" in {
    val dispatcher = StrategyDispatcher.getOrCreate(configStr, mapping, "config")
    val strategies = dispatcher.strategies
    strategies.map{
      case (k,v) =>
        println(k)
        dispatcher.dispatch(k)
    }
  }
}
