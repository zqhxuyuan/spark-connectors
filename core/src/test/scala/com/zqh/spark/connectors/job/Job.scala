package com.zqh.spark.connectors.job

/**
  * Created by zhengqh on 17/9/6.
  */
class Job {
  var jobId: String = _
  var jobName: String = _

  def buildJob(builder: Builder) = {
    jobId = builder.jobId
    jobName = builder.jobName
    this
  }

  def newBuilder() = new Builder()

  final class Builder {
    self =>
    var jobId: String = _
    var jobName: String = _

    def withJobId(jobId: String) = {
      this.jobId = jobId
      self
    }

    def withJobName(jobName: String) = {
      this.jobName = jobName
      self
    }

    def build() = buildJob(this)
  }
}

object Job {
  def main(args: Array[String]) {
    val job = new Job().newBuilder().withJobId("testId").withJobName("testName").build()
    println(job.jobId)
  }
}
