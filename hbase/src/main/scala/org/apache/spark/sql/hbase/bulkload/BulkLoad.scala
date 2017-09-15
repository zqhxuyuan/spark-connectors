package org.apache.spark.sql.hbase.bulkload

import java.util.Comparator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.fs.HFileSystem
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile, HFileContextBuilder}
import org.apache.hadoop.hbase.regionserver.{BloomType, HStore, StoreFile}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, KeyValue, TableName}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * http://blog.csdn.net/qq_28890765/article/details/54688575
  *
  * !!!TEST NOT WORK!!!
  */
@deprecated
class HBaseBulkLoader(conf: Configuration) {
  //val conf = config.get()
  val fs = HFileSystem.get(conf)
  val tempConf = new Configuration(conf)
  tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f)
  val familyOptions = FamilyOptions(Algorithm.NONE, DataBlockEncoding.PREFIX_TREE, 65536)
  val contextBuilder = new HFileContextBuilder()
    .withCompression(familyOptions.compression)
    .withChecksumType(HStore.getChecksumType(conf))
    .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
    .withBlockSize(familyOptions.blockSize)
  if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
    contextBuilder.withIncludesTags(true)
  }
  contextBuilder.withDataBlockEncoding(familyOptions.dataBlockEncoding)
  val hfileContext = contextBuilder.build()

  /**
    * @param dir 文件存放路径(tableName)
    * @param columnFamilyName family对应的hfile文件存放路径，用于配合bulkLoad
    */
  def getHFileWriter(dir: String, columnFamilyName: String) = {
    new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
      .withBloomType(BloomType.NONE)
      .withComparator(KeyValue.COMPARATOR)
      .withFileContext(hfileContext)
      .withOutputDir(new Path(dir, columnFamilyName))
      .build()
  }

  /**
    * @param flatMap 将数据转化为rowKey ，value形式的函数
    */
  def bulkLoad[T](rdd: RDD[T],
                  flatMap: (T) => Iterator[(RowKey, Array[Byte])],
                  tableName: TableName,
                  dir: String,
                  family: String,
                  familyOption: FamilyOptions = familyOptions,
                  compactionExclude: Boolean = false,
                  maxSize: Long = HConstants.DEFAULT_MAX_FILE_SIZE): Unit = {
    val conn = ConnectionFactory.createConnection(conf)
    val regionLocator = conn.getRegionLocator(tableName)
    val startKeys = regionLocator.getStartKeys
    val nowTimeStamp = System.currentTimeMillis()
    val regionSplitPartitioner = new BulkLoadPartitioner(startKeys)
    rdd.flatMap(r => flatMap(r)).repartitionAndSortWithinPartitions(regionSplitPartitioner).foreachPartition { part =>
      val writer = getHFileWriter(dir, family)
      while (part.hasNext) {
        val kv = part.next()
        writer.append(new KeyValue(kv._1.rowKey, kv._1.family, kv._1.qualifier, nowTimeStamp, KeyValue.Type.Put, kv._2))
      }
      writer.close()
    }
  }
}

// 按照region分区
class BulkLoadPartitioner(startKeys: Array[Array[Byte]]) extends Partitioner {
  override def numPartitions: Int = startKeys.length

  override def getPartition(key: Any): Int = {
    val comparator: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
      override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
        Bytes.compareTo(o1, o2)
      }
    }
    val rowKey: Array[Byte] =
      key match {
        case qualifier: RowKey => qualifier.rowKey
        case _ => key.asInstanceOf[Array[Byte]]
      }
    val partition = java.util.Arrays.binarySearch(startKeys, rowKey, comparator)
    if (partition < 0) partition * -1 + -2
    else partition
  }
}

// 压缩算法（LZO，SNAPPY）, block压缩算法（trie树等）,block大小
case class FamilyOptions(compression: Algorithm, dataBlockEncoding: DataBlockEncoding, blockSize: Int)

case class RowKey(rowKey: Array[Byte], family: Array[Byte], qualifier: Array[Byte]) extends Comparable[RowKey] with Serializable {
  override def compareTo(o: RowKey): Int = {
    var result = Bytes.compareTo(rowKey, o.rowKey)
    if (result == 0) {
      result = Bytes.compareTo(family, o.family)
      if (result == 0) result = Bytes.compareTo(qualifier, o.qualifier)
    }
    result
  }
  override def toString: String = {
    Bytes.toString(rowKey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
  }
}

class ConfigSerDeser(var conf: Configuration) extends Serializable {

  def this() {
    this(new Configuration())
  }

  def get(): Configuration = conf

  private def writeObject (out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject (in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}
