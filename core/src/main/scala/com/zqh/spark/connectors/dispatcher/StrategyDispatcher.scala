package com.zqh.spark.connectors.dispatcher

import java.util.concurrent.atomic.AtomicReference
import com.alibaba.fastjson.JSON
import com.zqh.spark.connectors.config.ConfigUtils
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

/**
  * https://github.com/allwefantasy/ServiceframeworkDispatcher
  *
  * 1. use fastjson
  * 2. use java conversion to convert json
  */
class StrategyDispatcher[T] {

  final val STRATEGY = "strategy"
  final val PROCESSOR = "processor"
  final val ALGORITHM = "algorithm"
  final val COMPOSITOR = "compositor"

  // jobName -> strategy
  private val _strategies = new HMap[String, Strategy[T]]()
  private val logger = LoggerFactory.getLogger(classOf[StrategyDispatcher[T]])

  // jobName -> configItem -> config
  var _config: JMap[String, JMap[String, Any]] = _ // new HMap[String, JMap[String, Any]]()

  def strategies = _strategies

  private var shortNameJMapping: ShortNameMapping = new ShortNameMapping {
    override def forName(shortName: String): String = shortName
  }

  def configShortNameJMapping(mapping: ShortNameMapping) = {
    shortNameJMapping = mapping
  }

  def shortName2FullName(config: JMap[String, Any], key: String = "name") = {
    require(config.contains(key), s"配置信息${config}必须包含配置键${key}")
    val shortName = config(key).asInstanceOf[String]
    shortNameJMapping.forName(shortName)
  }

  // 重新读取配置
  def reload(configStr: String) = {
    synchronized {
      _strategies.foreach(_._2.stop)
      loadConfig(configStr)
    }
  }

  /**
    * 读取配置
    * 通过FastJSON或者Typesafe Config解析为java.util.Map[String, java.util.Map[String, Any]]
    * 其中Any会再进一步在使用时, 通过asInstanceOf[]实例化为其他的类型, 比如List[Map[String, String]]等
    */
  def loadConfig(configStr: String, format: String = "json") = {
    _config = format match {
      case "json" =>
        val content = if(configStr == null) {
          ConfigUtils.loadConfigFile2String("strategy.v2.json")
        } else configStr

        JSON.parse(content).asInstanceOf[JMap[String, JMap[String, Any]]]
      case "config" =>
        ConfigUtils.loadConfigInnerMap("strategy.v2.json")
    }

    _config.foreach { f =>
      createStrategy(f._1, f._2)
    }
  }

  // 创建策略
  def createStrategy(jobName: String, jobJMap: JMap[String, Any]): Option[Strategy[T]] = {
    if (_strategies.contains(jobName)) return None // 策略已经存在, 立即返回
    require(jobJMap.contains("strategy"), s"$jobName 必须包含 strategy 字段。该字段定义策略实现类")

    // 利用反射机制, 创建与shortName对应的策略实现类. shortNameJMapping定义了shortName与fullName的映射关系
    val strategy = Class.forName(shortName2FullName(jobJMap, "strategy")).newInstance().asInstanceOf[Strategy[T]]
    val configParams: JMap[String, Any] = if (jobJMap.contains("configParams")) jobJMap("configParams").asInstanceOf[JMap[String, Any]] else new HMap[String, Any]()
    strategy.initialize(jobName, createAlgorithms(jobJMap), createRefs(jobJMap), createCompositors(jobJMap), configParams)
    _strategies.put(jobName, strategy)
    Option(strategy)
  }

  // 创建算法。一个策略由0个或者多个算法提供结果
  private def createAlgorithms(jobJMap: JMap[String, Any]): JList[Processor[T]] = {
    if (!jobJMap.contains("algorithm") && !jobJMap.contains("processor")) return new AList[Processor[T]]()
    val processors = if (jobJMap.contains("algorithm")) jobJMap("algorithm") else jobJMap("processor")
    processors.asInstanceOf[JList[JMap[String, Any]]].map {
      alg =>
        val name = shortName2FullName(alg)
        val processor = Class.forName(name).newInstance().asInstanceOf[Processor[T]]
        val configParams: JList[JMap[String, Any]] = if (alg.contains("params")) alg("params").asInstanceOf[JList[JMap[String, Any]]] else new AList[JMap[String, Any]]()
        processor.initialize(name, configParams)
        processor
    }
  }

  // 创建策略。一个策略允许混合包括算法，其他策略提供的结果。
  private def createRefs(jobJMap: JMap[String, Any]): JList[Strategy[T]] = {
    val result = new AList[Strategy[T]]()
    if (!jobJMap.contains("ref")) return result
    jobJMap("ref").asInstanceOf[JList[String]].foreach {
      refName =>
        if (_strategies.contains(refName)) {
          result.add(_strategies(refName))
        } else {
          _config.toMap.get(refName) match {
            case Some(jobMap) =>
              createStrategy(refName, jobMap) match {
                case Some(i) => result.add(i)
                case None =>
              }
            case None =>
          }
        }
    }
    result
  }

  // 创建组合器，可以多个，按顺序调用。有点类似过滤器链。第一个过滤器会接受算法或者策略的结果。后续的组合器就只能处理上一阶段的组合器吐出的结果
  private def createCompositors(jobJMap: JMap[String, Any]): JList[Compositor[T]] = {
    if (!jobJMap.contains("compositor")) return new AList()
    val compositors = jobJMap.get("compositor")
    compositors.asInstanceOf[JList[JMap[String, Any]]].map {
      f =>
        val compositor = Class.forName(shortName2FullName(f)).newInstance().asInstanceOf[Compositor[T]]
        val configParams: JList[JMap[String, Any]] = if (f.contains("params")) f.get("params").asInstanceOf[JList[JMap[String, Any]]] else new AList[JMap[String, Any]]()
        compositor.initialize(f.get("typeFilter").asInstanceOf[JList[String]], configParams)
        compositor
    }
  }

  // 调用链
  def dispatch(jobName: String): JList[T] = {
    val strategy = findStrategies(jobName)
    val params = _config(jobName)
    strategy match {
      case Some(strategies) =>
        val result = new AList[T]()
        try {
          result.addAll(strategies.flatMap { f =>
            val time = System.currentTimeMillis()
            val res = f.result(params)
            logger.info( s"""${params.get("_token_")} ${f.name} ${System.currentTimeMillis() - time}""")
            res
          })
        } catch {
          case e: Exception => logger.error("调用链路异常", e)
        }
        result
      case None => new AList[T]()
    }
  }

  def findStrategies(key: String): Option[JList[Strategy[T]]] = {
    val strategy = _strategies.get(key)
    val list = new AList[Strategy[T]]()
    list.add(strategy)
    Option(list)
  }

}

trait ShortNameMapping {
  def forName(shortName: String): String
}

object StrategyDispatcher {
  private val INSTANTIATION_LOCK = new Object()

  @transient private val lastInstantiatedContext = new AtomicReference[StrategyDispatcher[Any]]()

  def getOrCreate(configFile: String, shortNameJMapping: ShortNameMapping, format: String = "json"): StrategyDispatcher[Any] = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        val temp = new StrategyDispatcher[Any]()
        if(shortNameJMapping != null){
          temp.configShortNameJMapping(shortNameJMapping)
        }
        temp.loadConfig(configFile, format)
        setLastInstantiatedContext(temp)
      }
    }
    lastInstantiatedContext.get()
  }

  def clear = lastInstantiatedContext.set(null)

  private def setLastInstantiatedContext(strategyDispatcher: StrategyDispatcher[Any]): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(strategyDispatcher)
    }
  }
}


