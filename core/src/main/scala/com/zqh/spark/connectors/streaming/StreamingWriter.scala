package com.zqh.spark.connectors.streaming

import com.zqh.spark.connectors.SparkWriter
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingWriter(config: Map[String, String]) extends SparkWriter{
  override def write(df: DataFrame): Unit = {
    val format = config.getOrElse("format", "")

    val query = df.writeStream.format(format).options(config - format).start()
    query.awaitTermination()
  }
}

object StreamingWriter {
  def apply(config: Map[String, String]) = new StreamingWriter(config)
}