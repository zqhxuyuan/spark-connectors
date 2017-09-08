package org.apache.spark.sql.codis

import com.zqh.spark.connectors.ConnectorParameters
import io.codis.jodis.{JedisResourcePool, RoundRobinJedisPool}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import redis.clients.jedis.Jedis

class DefaultSource extends DataSourceRegister
  with CreatableRelationProvider
  with SchemaRelationProvider
  with RelationProvider
  with Serializable {

  override def shortName(): String = "codis"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    new CodisRelation(schema)(sqlContext.sparkSession)
  }

  override def createRelation(sqlContext: SQLContext,
                              saveMode:   SaveMode,
                              parameters: Map[String, String],
                              dataframe:  DataFrame): BaseRelation = {
    import ConnectorParameters.Codis._
    val zkHost = parameters(codisZkHost)
    val zkTimeout = parameters.getOrElse(codisZkTimeout, "30000").toInt
    val zkProxyDir = parameters(codisZkDir)
    val password = parameters(codisPassword)
    val filter = parameters.getOrElse(codisFilter, "")
    val ttl = parameters.getOrElse(codisFilter, "")
    val prefix = parameters.getOrElse(codisPrefix, "")
    val operator = parameters(codisCommand)

    dataframe.foreachPartition(rows=>{
      val jedisPool: JedisResourcePool = RoundRobinJedisPool.create.curatorClient(zkHost, zkTimeout).zkProxyDir(zkProxyDir).build
      val jedis: Jedis = jedisPool.getResource
      if(!password.equals("")) jedis.auth(password)
      rows.foreach(row=>{
        val argsCount = getArgsCount(operator)
        val args = argsCount match {
          case 1 => // operator key arg1
            List(prefix + row.getAs[String](0), row.getAs[String](1))
          case 2 => // operator key arg1 arg2
            List(prefix + row.getAs[String](0), row.getAs[String](1), row.getAs[String](2))
          case _ =>
            println("no support types...")
            null
        }
        saveRedisScript(jedis, operator, args, filter, ttl)
      })
      jedis.close()
    })
    new CodisRelation(dataframe.schema)(sqlContext.sparkSession)
  }

  def getArgsCount(opeartor: String) = {
    opeartor match {
      case "set" => 1
      case "hset" => 2
      case "lpush" => 1
      case "sadd" => 1
      case "zadd" => 2
    }
  }

  def saveRedisScript(jedis: Jedis, operator: String, args: List[String], filter: String = "", ttl: String = ""): Unit = {
    // 自定义脚本
    if(!filter.equals("")) {
      jedis.eval(filter, 1, args: _*)
    } else{
      val script = operator.toLowerCase() match {
        case "set" => "redis.call('set', KEYS[1], ARGV[1])"
        case "hset" => "redis.call('hset', KEYS[1], ARGV[1], ARGV[2])"
        case "lpush" => "redis.call('lpush', KEYS[1], ARGV[1])"
        case "sadd" => "redis.call('sadd', KEYS[1], ARGV[1])"
        case "zadd" => "redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])"
        case _ => ""
      }
      jedis.eval(script, 1, args: _*)
    }
    // 支持TTL
    if(!ttl.equals("")) jedis.expire(args(0), ttl.toInt)
  }

}