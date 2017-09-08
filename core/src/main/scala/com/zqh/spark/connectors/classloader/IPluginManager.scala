package com.zqh.spark.connectors.classloader

import scala.collection.mutable

/**
  * Created by zhengqh on 17/9/6.
  */
trait IPluginManager {
  var pluginMap = mutable.Map[String, PluginClassLoader]()

  def addLoader(pluginName: String, plugin: PluginClassLoader) = pluginMap += (pluginName -> plugin)

  def getLoader(pluginName: String) = pluginMap.getOrElse(pluginName, new PluginClassLoader())

  def loadPlugin(pluginName: String): PluginClassLoader

  def unloadPlugin(pluginName: String) = {
    val pluginOpt = pluginMap.get(pluginName)
    pluginOpt match {
      case Some(plugin) =>
        plugin.unloadJarFiles
        pluginMap -= pluginName
      case None =>
    }
  }
}
