package com.zqh.spark.connectors.config

import com.typesafe.config.{ConfigValue, ConfigFactory}
import scala.collection.JavaConversions._

import org.scalatest._

/**
  * Created by zhengqh on 17/8/31.
  */
class TestTypesafeConfig extends FlatSpec with Matchers{

  "Simple Test" should "simple" in {
    val conf = ConfigFactory.load()
    val readType = conf.getString("reader.format")
    println(readType)

    val pipeConf = ConfigFactory.load("pipeline")
    val readers = pipeConf.getConfig("readers")
    val entries = readers.entrySet()
    for(entry <- entries) {
      println(entry.getKey + ": " + entry.getValue)
    }
    println(readers.getString("jdbc1.table"))

    val connectors = ConfigUtils.loadConfig("complex")
    println(connectors("readers").mkString("\n"))
    println(connectors("writers").mkString("\n"))
    val readerConfigs = connectors("readers")
    val writerConfigs = connectors("writers")
  }

  "unwrap and instance" should "work" in {
    val config = ConfigFactory.load("complex")
    val complexStructure = config.getList("connectors").unwrapped().
      asInstanceOf[java.util.List[java.util.Map[String, java.util.List[java.util.Map[String, String]]]]]

    def java2scala(list: java.util.List[java.util.Map[String, java.util.List[java.util.Map[String, String]]]]) {
      list.map(l => l.toList.map(m => m._1 -> m._2.toList.map(ll => ll.toMap)))
    }

    val complexMap = complexStructure(0)
    println(complexMap)
    assert(complexMap("readers")(0)("table").equals("test"))
  }

  "Config and ConfigObject" should "json" in {
    // TODO how to convert json to Map[String, Map[String, Any]]
    val jobConfig = ConfigFactory.load("strategy.v2.json")

    // 迭代Config接口
    // When iterating a Config you will not find nested Config, because everything gets flattened into one level.
    val sysProperties = System.getProperties
    val configMap: java.util.Set[java.util.Map.Entry[String, ConfigValue]] =
      jobConfig.entrySet().filter { entry =>
        !sysProperties.containsKey(entry.getKey)
    }
    // Set(convert-multi-csv-to-json.desc=Quoted("测试1"),
    //     convert-multi-csv-to-json.algorithm=SimpleConfigList([{"name":"testProcessor"},{"name":"testProcessor"}]),
    //     convert-multi-csv-to-json2.compositor=SimpleConfigList([{"name":"testCompositor"}]),
    //     convert-multi-csv-to-json2.algorithm=SimpleConfigList([{"name":"testProcessor"}]),
    //     convert-multi-csv-to-json2.ref=SimpleConfigList([]),
    //     convert-multi-csv-to-json.compositor=SimpleConfigList([{"name":"testCompositor"},{"name":"testCompositor"}]),
    //     convert-multi-csv-to-json.ref=SimpleConfigList(["convert-multi-csv-to-json2"]),
    //     convert-multi-csv-to-json.configParams.k1=ConfigInt(1),
    //     convert-multi-csv-to-json.strategy=Quoted("spark"),
    //     convert-multi-csv-to-json2.strategy=Quoted("spark"),
    //     convert-multi-csv-to-json2.desc=Quoted("测试2"),
    //     convert-multi-csv-to-json.configParams.k2=Quoted("2"))
    println(configMap)

    // 迭代ConfigObject接口.
    val configObjectMap = jobConfig.root().entrySet().filter{ entry =>
      !sysProperties.containsKey(entry.getKey)
    }
    println(configObjectMap)
  }

  "Parse JSON java way" should "parse json" in {
    val jobConfig = ConfigFactory.load("strategy.v2.json")
    val sysProperties = System.getProperties
    val root = jobConfig.entrySet().filter(entry => !sysProperties.containsKey(entry.getKey))

    val map = jobConfig.root.unwrapped().
      asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]].
      filter(_._1.contains("-")) // only self defined properties, filter system properties

    println(map("convert-multi-csv-to-json"))
  }

  "parsing inner map" should "deep" in {
    val jobConfig = ConfigFactory.load("strategy.v2.json")
    val sysProperties = System.getProperties
    val root = jobConfig.entrySet().filter(entry => !sysProperties.containsKey(entry.getKey))

    val map = jobConfig.root.unwrapped().
      asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]]
      //filter(_._1.contains("-"))

    val algsCfg = map("convert-multi-csv-to-json")("algorithm")
    val algsIns = algsCfg.asInstanceOf[java.util.List[java.util.Map[String, Any]]]
    println(algsIns)

    val refsCfg = map("convert-multi-csv-to-json")("ref")
    val refs = refsCfg.asInstanceOf[java.util.List[String]]
    refs.toList should be (List("convert-multi-csv-to-json2"))
  }

  "Parse JSON scala way" should "parse json" in {
    val jobConfig = ConfigFactory.load("strategy.v2.json")
    val sysProperties = System.getProperties
    val root = jobConfig.entrySet().filter(entry => !sysProperties.containsKey(entry.getKey))

    // com.typesafe.config.impl.SimpleConfigObject cannot be cast to scala.collection.immutable.Map
    assertThrows[ClassCastException] {
      val map = jobConfig.root.
        asInstanceOf[Map[String, Map[String, Any]]].
        filter(_._1.contains("-")) // only self defined properties, filter system properties

      println(map("convert-multi-csv-to-json"))
      println(map("convert-multi-csv-to-json2"))
    }

    // SimpleConfigObject({"algorithm":[{"name":"testProcessor"},{"name":"testProcessor"}],"compositor":[{"name":"testCompositor"},{"name":"testCompositor"}],"configParams":{"k1":1,"k2":"2"},"desc":"测试1","ref":["convert-multi-csv-to-json2"],"strategy":"spark"})
    val map = jobConfig.root.
      asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]].
      filter(_._1.contains("-"))
    println(map("convert-multi-csv-to-json"))

  }
}
