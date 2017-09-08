package com.zqh.spark.connectors.util

/**
  * Created by zhengqh on 17/9/6.
  */
object TimeUtils {

  def now() = System.currentTimeMillis()

  def time(desc: String = "")(f: => Unit): Unit = {
    val start = now()
    f
    println(desc + (now() - start) + "ms")
  }
}
