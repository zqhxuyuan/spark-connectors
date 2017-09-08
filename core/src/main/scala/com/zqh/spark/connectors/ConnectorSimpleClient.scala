package com.zqh.spark.connectors

import com.zqh.spark.connectors.config.{ConfigUtils}
import com.zqh.spark.connectors.engine.{FlinkEngine, SparkEngine}

object ConnectorSimpleClient {

  def main(args: Array[String]) {
    val connectors = ConfigUtils.loadConfig()
    val readerConfigs = connectors("readers")
    val writerConfigs = connectors("writers")

    val engine = if(args(0) != null) args(0) else "spark"

    engine match {
      case "spark" => SparkEngine.run(readerConfigs, writerConfigs)
      case "flink" => FlinkEngine.run(readerConfigs, writerConfigs)
    }

  }
}
