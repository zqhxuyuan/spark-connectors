package com.zqh.spark.connectors.core

import com.zqh.spark.connectors.{SparkTransformer, SparkReader, SparkWriter}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
class SparkConnectors(reader: SparkReader,
                      writer: SparkWriter,
                      transformer: SparkTransformer,
                      spark: SparkSession) {
  def runSparkJob(): Unit = {
    // 初始化
    reader.init(spark)

    // 读取源
    val source = reader.read(spark)

    // 转换操作
    val transform = transformer.transform(source)

    // 写入目标
    writer.write(transform)

    // 关闭
    writer.close()

    spark.close()
  }
}
