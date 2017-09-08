package com.zqh.spark.connectors.core

import com.zqh.spark.connectors.{SparkReader, SparkWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/30.
  */
class SparkPipelines(readers: List[SparkReader], writers: List[SparkWriter], spark: SparkSession){

  def runSparkJob(): Unit = {
    // 初始化
    readers.foreach(reader => reader.init(spark))

    // 读取源
    var union: DataFrame = null
    readers.foreach(reader => {
      val source = reader.read(spark)
      if(union == null) union = source
      else union = union.union(source)
    })

    // 写入目标
    writers.foreach(writer => {
      writer.write(union)
      writer.close()
    })

    // 关闭
    spark.close()
  }
}
