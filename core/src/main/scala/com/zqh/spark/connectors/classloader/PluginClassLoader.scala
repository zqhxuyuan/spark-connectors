package com.zqh.spark.connectors.classloader

import java.net.{URLClassLoader, URL, JarURLConnection, URLConnection}

import sun.net.www.protocol.file.FileURLConnection

import scala.collection.mutable
//import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader

/**
  * Created by zhengqh on 17/9/6.
  */
class PluginClassLoader(url: Array[URL], classLoader: ClassLoader) extends URLClassLoader(url, classLoader){

  var cachedJarFiles = mutable.MutableList[JarURLConnection]()
  var cachedFileJar = mutable.MutableList[FileURLConnection]()

  def this() {
    this(Array[URL](), Thread.currentThread().getContextClassLoader)
  }

  def this(url: Array[URL]) {
    this(url, Thread.currentThread().getContextClassLoader)
  }

  def addURLFile(file: URL) {
    try {
      val uc: URLConnection = file.openConnection
      if (uc.isInstanceOf[JarURLConnection]) {
        println(s"CACHING ${file.getProtocol.toUpperCase()} | ${file}")
        uc.setUseCaches(true)
        val jar = uc.asInstanceOf[JarURLConnection]
        jar.getManifest
        cachedJarFiles += jar
      } else if(uc.isInstanceOf[FileURLConnection]) {
        println(s"CACHING ${file.getProtocol.toUpperCase()} | ${file}")
        uc.setUseCaches(true)
        val jar = uc.asInstanceOf[FileURLConnection]
        cachedFileJar += jar
      }
      //TODO HDFS FsUrlConnection
      //else if(uc.isInstanceOf[FsUrlConnection]) {}
    } catch {
      case e: Exception => System.err.println("Failed to cache plugin JAR file: " + file.toExternalForm)
    }
    addURL(file)
  }

  def unloadJarFiles() {
    for (url <- cachedJarFiles) {
      try {
        url.getJarFile.close
      } catch {
        case e: Exception => System.err.println("Failed to unload JAR file\n" + e)
      }
    }

    for(file <- cachedFileJar) {
      file.close()
    }
  }
}