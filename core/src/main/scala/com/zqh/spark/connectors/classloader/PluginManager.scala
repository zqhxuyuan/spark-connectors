package com.zqh.spark.connectors.classloader

import java.io.File
import java.net.{MalformedURLException, URL}

import org.apache.hadoop.fs.{Path, FsUrlStreamHandlerFactory}

/**
  * Created by zhengqh on 17/9/6.
  */
class PluginManager extends IPluginManager{
  override def loadPlugin(pluginName: String): PluginClassLoader = {
    println(s"pluginName: $pluginName")
    pluginMap -= pluginName
    val loader = new PluginClassLoader
    try {
      val url = pluginName match {
        case p if(p.startsWith("hdfs")) =>
          try {
            val fsUrlStreamHandlerFactory = new FsUrlStreamHandlerFactory()
            URL.setURLStreamHandlerFactory(fsUrlStreamHandlerFactory)
          } catch {
            // java.lang.Error: factory already defined
            case e: java.lang.Error => e.printStackTrace()
            case e: Exception => e.printStackTrace()
          }
          new Path(pluginName).toUri.toURL
        case p if(p.startsWith("/")) =>
          new File(pluginName).toURI.toURL
        case _ =>
          new URL(pluginName)
      }

      loader.addURLFile(url)
      addLoader(pluginName, loader)
    } catch {
      case e: MalformedURLException => e.printStackTrace
    }
    loader
  }
}
