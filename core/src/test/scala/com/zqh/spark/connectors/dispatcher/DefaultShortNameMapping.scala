package com.zqh.spark.connectors.dispatcher

class DefaultShortNameMapping extends ShortNameMapping {
  private val compositorNameMap: Map[String, String] = Map[String, String](
    "spark" -> "com.zqh.spark.connectors.dispatcher.TestStrategy",
    "testProcessor" -> "com.zqh.spark.connectors.dispatcher.TestProcessor",
    "testCompositor" -> "com.zqh.spark.connectors.dispatcher.TestCompositor"
  )

  override def forName(shortName: String): String = {
    if (compositorNameMap.contains(shortName)) {
      compositorNameMap(shortName)
    } else {
      shortName
    }
  }
}