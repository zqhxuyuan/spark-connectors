package com.zqh.spark.connectors.client

import com.zqh.spark.connectors.config.ConfigUtils
import com.zqh.spark.connectors.engine.{FlinkEngine, KafkaEngine, SparkEngine}

object ConnectorSimpleClient {

  def main(args: Array[String]) {
    val connectors = ConfigUtils.loadConfig()
    val readerConfigs = connectors("readers")
    val writerConfigs = connectors("writers")
    val transformerConfigs = connectors("transformers")

    val engine = if(args(0) != null) args(0) else "spark"


    engine match {
      case "spark" => SparkEngine.run(readerConfigs, writerConfigs, transformerConfigs)
      case "flink" => FlinkEngine.run(readerConfigs, writerConfigs)
      case "kafka" => KafkaEngine.run(readerConfigs, writerConfigs)
    }

  }
}
