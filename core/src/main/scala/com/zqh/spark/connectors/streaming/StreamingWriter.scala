package com.zqh.spark.connectors.streaming

import com.zqh.spark.connectors.df.SparkDFWriter
import com.zqh.spark.connectors.util.ConnectorUtils._
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingWriter(config: Map[String, String]) extends SparkDFWriter{
  override def writeDF(df: DataFrame): Unit = {
    val format = config.getOrElse("format", "")
    var writer = df.writeStream
    if(!format.equals("")) writer = writer.format(format)

    //val foreach = config.getOrElse("foreach", "")
    //if(!foreach.equals("")) writer = writer.foreach()

    val query = writer.options(config - format).start()
    StreamingManager.register(query.id, query)
    query.awaitTermination()
  }
}

object StreamingWriter {
  def apply(config: Map[String, String]) = new StreamingWriter(config)
}