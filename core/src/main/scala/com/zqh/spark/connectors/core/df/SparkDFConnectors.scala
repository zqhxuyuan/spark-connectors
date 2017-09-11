package com.zqh.spark.connectors.core.df

import com.zqh.spark.connectors.df.{SparkDFReader, SparkDFTransformer, SparkDFWriter}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
class SparkDFConnectors(reader: SparkDFReader,
                        writer: SparkDFWriter,
                        transformer: SparkDFTransformer,
                        spark: SparkSession) {
  def runSparkJob(): Unit = {
    // 初始化
    reader.init(spark)

    // 读取源
    val source = reader.readDF(spark)

    // 转换操作
    val transform = transformer.transformDF(source)

    // 写入目标
    writer.writeDF(transform)

    // 关闭
    writer.close()

    spark.close()
  }
}
