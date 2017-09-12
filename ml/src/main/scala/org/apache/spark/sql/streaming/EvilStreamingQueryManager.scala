package org.apache.spark.sql.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink

case class EvilStreamingQueryManager(streamingQueryManager: StreamingQueryManager) {
  def startQuery(
                  userSpecifiedName: Option[String],
                  userSpecifiedCheckpointLocation: Option[String],
                  df: DataFrame,
                  sink: Sink,
                  outputMode: OutputMode): StreamingQuery = {
    streamingQueryManager.startQuery(
      userSpecifiedName,
      userSpecifiedCheckpointLocation,
      df,
      sink,
      outputMode)
  }
}
