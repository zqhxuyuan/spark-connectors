package com.zqh.spark.connectors.util

import com.google.common.util.concurrent.RateLimiter
import TimeUtils._

/**
  * Created by zhengqh on 17/9/6.
  */
object TestGuavaRateLimiter {

  def main(args: Array[String]) {

    time("not rate: ") {
      for(i <- 0 until 10) {
        println("calling...")
      }
    }

    time("guava rate") {
      val limiter = RateLimiter.create(10.0)
      for(i <- 0 until 100) {
        limiter.acquire()
        println("calling...")
      }
    }

  }
}
