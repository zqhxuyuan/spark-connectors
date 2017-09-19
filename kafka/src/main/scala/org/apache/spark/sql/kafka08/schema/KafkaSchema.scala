package org.apache.spark.sql.kafka08.schema

import org.apache.spark.sql.types._

/**
  * Created by zhengqh on 17/9/18.
  */
object KafkaSchema {

  val SCHEMA = "schema"

  def defaultSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType)
  ))

  def getKafkaSchema(kafkaParams: Map[String, String]) = {
    val transformSchema = kafkaParams.getOrElse(SCHEMA, "")
    if(transformSchema.equals("")) defaultSchema
    else DataframeSchema.buildSimpleSchema(transformSchema)
  }
}
