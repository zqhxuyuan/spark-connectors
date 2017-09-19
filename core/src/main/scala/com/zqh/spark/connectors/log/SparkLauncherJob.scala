package com.zqh.spark.connectors.log

import java.io.InputStreamReader

import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.event.Level

// http://henningpetersen.com/post/22/running-apache-spark-jobs-from-applications
class SparkLauncherJob(process: Process) {

  private val logger = LoggerFactory.getLogger(classOf[SparkLauncherJob])

  def exitCode: Int = {
    process.waitFor()
  }

  def cancel(): Unit ={
    process.destroy()
  }

  def log(jobLogger: Logger): Unit ={
    val errorStream  = process.getErrorStream
    val infoStream = process.getInputStream
    val errorReader = new InputStreamReader(errorStream)
    val infoReader = new InputStreamReader(infoStream)
    val infoGobbler = new LogGobbler(infoReader, jobLogger, Level.INFO, 100)
    val errorGobbler = new LogGobbler(errorReader, jobLogger, Level.ERROR, 100)
    infoGobbler.start()
    errorGobbler.start()
    infoGobbler.awaitCompletion(100)
    errorGobbler.awaitCompletion(100)
  }
}
