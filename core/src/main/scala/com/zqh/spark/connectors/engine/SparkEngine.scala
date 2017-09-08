package com.zqh.spark.connectors.engine

import com.zqh.spark.connectors.core.SparkPipelines
import com.zqh.spark.connectors.{SparkWriter, SparkReader}
import com.zqh.spark.connectors.dataframe.{DFWriter, DFReader}
import com.zqh.spark.connectors.streaming.{StreamingWriter, StreamingReader}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/9/8.
  */
object SparkEngine {

  def run(readerConfigs: List[Map[String, String]], writerConfigs: List[Map[String, String]]) {
    val readerConnectors = readerConfigs.map(createReader(_))
    val writerConnectors = writerConfigs.map(createWriter(_))
    val spark = SparkSession.builder().getOrCreate()
    val job = new SparkPipelines(readerConnectors, writerConnectors, spark)
    job.runSparkJob()
  }

  def createReader(config: Map[String, String]): SparkReader = {
    if(config("format").startsWith("ss.")) StreamingReader(config)
    else DFReader(config)
  }

  def createWriter(config: Map[String, String]): SparkWriter = {
    if(config("format").startsWith("ss.")) StreamingWriter(config)
    else DFWriter(config)
  }
}
