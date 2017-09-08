package com.zqh.spark.connectors.util
import TimeUtils._

/**
  * Created by zhengqh on 17/9/6.
  */
object TestRateLimiter {

  val rateLimiter = new RateLimit(5 * 1024, 1 * 1024)

  def main(args: Array[String]) {
    time() {
      rateLimiter.maybeSleep(5 * 1024)
    }

    time() {
      rateLimiter.maybeSleep(10 * 1024)
    }

    time() {
      rateLimiter.maybeSleep(15 * 1024)
    }
  }
}
