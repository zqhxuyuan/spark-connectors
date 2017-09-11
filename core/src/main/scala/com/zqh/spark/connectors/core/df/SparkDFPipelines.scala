package com.zqh.spark.connectors.core.df

import com.zqh.spark.connectors.df.{SparkDFReader, SparkDFWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/30.
  */
class SparkDFPipelines(readers: List[SparkDFReader], writers: List[SparkDFWriter], spark: SparkSession){

  def runSparkJob(): Unit = {
    // 初始化
    readers.foreach(reader => reader.init(spark))

    // 读取源
    var union: DataFrame = null
    readers.foreach(reader => {
      val source = reader.readDF(spark)
      if(union == null) union = source
      else union = union.union(source)
    })

    // 写入目标
    writers.foreach(writer => {
      writer.writeDF(union)
      writer.close()
    })

    // 关闭
    spark.close()
  }
}
