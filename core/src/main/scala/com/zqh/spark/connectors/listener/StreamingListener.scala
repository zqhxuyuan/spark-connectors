package com.zqh.spark.connectors.listener

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryStartedEvent, QueryTerminatedEvent, QueryProgressEvent}

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingListener extends StreamingQueryListener{
  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    println("Query started: " + queryStarted.id)
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    println("Query terminated: " + queryTerminated.id)
  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    println("Query made progress: " + queryProgress.progress)
  }
}
