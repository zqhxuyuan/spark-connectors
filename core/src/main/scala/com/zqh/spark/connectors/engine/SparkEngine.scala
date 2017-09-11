package com.zqh.spark.connectors.engine

import com.zqh.spark.connectors.core.SparkPipelines
import com.zqh.spark.connectors.core.df.SparkDFPipelines
import com.zqh.spark.connectors.df.{SparkDFWriter, SparkDFReader}
import com.zqh.spark.connectors.sql.{SqlTransformer, SparkSQLTransformer, SqlWriter, SqlReader}
import com.zqh.spark.connectors.{ISparkTransformer, ISparkReader, ISparkWriter}
import com.zqh.spark.connectors.dataframe.{DFWriter, DFReader}
import com.zqh.spark.connectors.streaming.{StreamingWriter, StreamingReader}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/9/8.
  */
object SparkEngine {

  def run(readerConfigs: List[Map[String, String]],
          writerConfigs: List[Map[String, String]],
          transformerConfigs: List[Map[String,String]]) {
    val readerConnectors = readerConfigs.map(createReader(_))
    val writerConnectors = writerConfigs.map(createWriter(_))
    val transformers = transformerConfigs.map(createTransformer(_))
    val spark = SparkSession.builder().getOrCreate()

    //val job = new SparkDFPipelines(readerConnectors, writerConnectors, spark)
    val job = new SparkPipelines(readerConnectors, writerConnectors, transformers, spark)
    job.runSparkJob()
  }

  def createReader(config: Map[String, String]): ISparkReader = {
    val format = config("format")
    format match {
      case _ if(format.startsWith(TYPE_SS)) => StreamingReader(removePrefixFormat(config))
      case _ if(format.startsWith(TYPE_SQL)) => SqlReader(removePrefixFormat(config))
      case _ => DFReader(config)
    }
  }

  def createWriter(config: Map[String, String]): ISparkWriter = {
    val format = config("format")
    format match {
      case _ if(format.startsWith(TYPE_SS)) => StreamingWriter(removePrefixFormat(config))
      case _ if(format.startsWith(TYPE_SQL)) => SqlWriter(removePrefixFormat(config))
      case _ => DFWriter(config)
    }
  }

  // TODO support dataframe transform
  def createTransformer(config: Map[String, String]): ISparkTransformer = {
    val format = config("format")
    format match {
      case _ if(format.startsWith(TYPE_SQL)) => SqlTransformer(removePrefixFormat(config))
      case _ => null
    }
  }

  val TYPE_SQL = "sql"
  val TYPE_SS = "ss."

  def removePrefixFormat(config: Map[String, String]) = {
    val configFormat = config("format")
    if(configFormat.startsWith(TYPE_SQL) || configFormat.startsWith(TYPE_SS)) {
      var format = config("format").replaceFirst(TYPE_SQL, "").replaceFirst(TYPE_SS, "")
      if(format.startsWith("."))
        format = format.replaceFirst(".", "")
      if(!format.equals(""))
        config + ("format" -> format)
      else
        config - "format"
    } else
      config
  }
}
