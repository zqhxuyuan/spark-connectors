package com.zqh.spark.connectors.job

class MyJob(val jobId: String = null, val jobName: String = null, val jobs: List[String] = null) {

  def newBuilder() = new MyJobBuilder(jobId, jobName, jobs)

  class MyJobBuilder(jobId: String = null,
                     jobName: String = null,
                     jobs: List[String] = Nil) {
    def withJobId(jobId: String) = new MyJobBuilder(jobId = jobId)
    def withJobName(jobName: String) = new MyJobBuilder(jobName = jobName)
    def withJobs(job: String) = new MyJobBuilder(jobs = job :: jobs)

    def build() = new MyJob(jobId, jobName, jobs)
  }
}

object MyJobApp {
  def main(args: Array[String]) {
    val myjob = new MyJob().newBuilder().withJobId("test").withJobName("test").withJobs("1").withJobs("2").build()
    println(myjob.jobs)
  }
}