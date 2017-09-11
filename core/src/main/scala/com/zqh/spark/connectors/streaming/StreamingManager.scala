package com.zqh.spark.connectors.streaming

import java.util.UUID

import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Created by zhengqh on 17/9/8.
  */
object StreamingManager {

  var listeners = Map[UUID, StreamingQuery]()

  def register(eventId: UUID, query: StreamingQuery) {
    listeners += eventId -> query
  }

  def getQuery(id: UUID) = listeners.get(id)
}

case class StreamingEvent(id: UUID, query: StreamingQuery)