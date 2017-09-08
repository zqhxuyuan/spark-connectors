package com.zqh.spark.connectors.config

import com.typesafe.config.{ConfigBeanFactory, ConfigFactory}
import scala.collection.JavaConversions._
import scala.io.Source
import java.util.{Map => JMap, HashMap => HMap, List => JList, ArrayList => AList}

/**
  * Created by zhengqh on 17/8/31.
  */
object ConfigUtils {

  private def toMap(hashMap: AnyRef): Map[String, AnyRef] = hashMap.asInstanceOf[java.util.Map[String, AnyRef]].toMap
  private def toList(list: AnyRef): List[AnyRef] = list.asInstanceOf[java.util.List[AnyRef]].toList

  def load(resourceName: String = "") = resourceName match {
    case _ if resourceName.equals("") => ConfigFactory.load()
    case _ => ConfigFactory.load(resourceName)
  }

  def loadConfig(resourceName: String = ""): Map[String, List[Map[String, String]]] = {
    val config = load(resourceName)
    val connectorsMap: Map[String, List[Map[String, String]]] =
      config.getList("connectors").unwrapped().map { someConfigItem =>
        toMap(someConfigItem) map { // writers => AnyRef
          case (key, value) =>
            key ->
              toList(value).map {  // AnyRef :=> List
                x => toMap(x).map {
                  case (k, v) => k -> v.toString
                }
              }
        }
      }.reduceLeft(_ ++ _)

    connectorsMap
  }

  def loadConfigFile2String(resourceName: String = "") = {
    Source.fromURL(getClass.getClassLoader.getResource(resourceName)).getLines().mkString
  }

  /**
    * TODO 这里为了区分自定义配置和系统配置, 自定义配置必须包含-
    * 否则没有过滤的话, 会包含系统配置, 而系统配置会导致ClassCastException: String can not cast to Map
    */
  def loadConfigInnerMap(resourceName: String = ""): JMap[String, JMap[String, Any]] = {
    val config = load(resourceName)
    config.root.unwrapped()
      .asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]].filter(_._1.contains("-"))
  }

  @deprecated
  def javaMap2Map(tmpConfig: java.util.Map[String, java.util.Map[String, Any]]): Map[String, Map[String, Any]] = {
    tmpConfig.toMap.map(mm => mm._1 -> mm._2.toMap)
  }

  def loadReaderConfigs(resourceName: String = "") = loadConfig(resourceName).get("readers")

  def loadWriterConfigs(resourceName: String = "") = loadConfig(resourceName).get("writers")
}
