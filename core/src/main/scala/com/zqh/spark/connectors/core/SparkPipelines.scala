package com.zqh.spark.connectors.core

import com.zqh.spark.connectors._
import com.zqh.spark.connectors.df.{SparkDFTransformer, SparkDFReader, SparkDFWriter}
import com.zqh.spark.connectors.sql.{SparkSQLWriter, SparkSQLTransformer, SparkSQLReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/30.
  */
class SparkPipelines(readers: List[ISparkReader],
                     writers: List[ISparkWriter],
                     transformers: List[ISparkTransformer],
                     spark: SparkSession){

  def runSparkJob(): Unit = {
    // 初始化
    readers.foreach(reader => {
      if(reader.isInstanceOf[SparkSQLReader]) {
        reader.asInstanceOf[SparkSQLReader].init(spark)
      } else if(reader.isInstanceOf[SparkDFReader]) {
        reader.asInstanceOf[SparkDFReader].init(spark)
      }
    })

    // 读取源
    var union: DataFrame = null
    readers.foreach(reader => {
      if(reader.isInstanceOf[SparkSQLReader]) {
        reader.asInstanceOf[SparkSQLReader].readSQL(spark)
      } else if(reader.isInstanceOf[SparkDFReader]) {
        //TODO UNION OPERATION MUST MAKE SURE SAME COLUMN
        val source = reader.asInstanceOf[SparkDFReader].readDF(spark)
        if(union == null) union = source
        else union = union.union(source)
      }
    })

    // 转换
    transformers.foreach(transformer => {
      if(transformer.isInstanceOf[SparkSQLTransformer]) {
        transformer.asInstanceOf[SparkSQLTransformer].transformSQL(spark)
      } else if(transformer.isInstanceOf[SparkDFTransformer]) {
        // TODO how to store transform intermediate result
        transformer.asInstanceOf[SparkDFTransformer].transformDF(union)
      }
    })

    // 写入目标
    writers.foreach(writer => {
      if(writer.isInstanceOf[SparkSQLWriter]) {
        val writerInstance = writer.asInstanceOf[SparkSQLWriter]
        writerInstance.writeSQL(spark)
        writerInstance.close()
      } else if(writer.isInstanceOf[SparkDFWriter]) {
        val writerInstance = writer.asInstanceOf[SparkDFWriter]
        writerInstance.writeDF(union)
        writerInstance.close()
      }
    })

    // 关闭
    spark.close()
  }
}
