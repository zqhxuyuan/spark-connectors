package com.zqh.spark.connectors.model

/**
  * Created by zhengqh on 17/9/7.
  */
object TestScalaBean {

  def main(args: Array[String]) {

    val bean = new ColField()
    bean.fromType = "jdbc"
    bean.toType = "jdbc"
    bean.fromField = "id"
    bean.toField = "id"

    println(bean.getFromType)
  }
}
