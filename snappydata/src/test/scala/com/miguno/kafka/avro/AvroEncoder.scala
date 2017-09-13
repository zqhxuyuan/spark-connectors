package com.miguno.kafka.avro

import java.io.ByteArrayOutputStream
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificRecordBase, SpecificDatumWriter}

/**
  * We must explicitly require the user to supply the schema -- even though it is readily available through T -- because
  * of Java's type erasure.  Unfortunately we cannot use Scala's `TypeTag` or `Manifest` features because these will add
  * (implicit) constructor parameters to actual Java class that implements T, and this behavior will cause Kafka's
  * mechanism to instantiate T to fail because it cannot find a constructor whose only parameter is a
  * `kafka.utils.VerifiableProperties`.  Again, this is because Scala generates Java classes whose constructors always
  * include the Manifest/TypeTag parameter in addition to the normal ones.  For this reason we haven't found a better
  * way to instantiate a correct `SpecificDatumWriter[T]` other than explicitly passing a `Schema` parameter.
  *
  * @param props Properties passed to the encoder.  At the moment the encoder does not support any special settings.
  * @param schema The schema of T, which you can get via `T.getClassSchema`.
  * @tparam T The type of the record, which must be backed by an Avro schema (passed via `schema`)
  */
class AvroEncoder[T <: SpecificRecordBase](props: VerifiableProperties = null, schema: Schema)
  extends Encoder[T] {

  private[this] val NoBinaryEncoderReuse = null.asInstanceOf[BinaryEncoder]
  private[this] val writer: DatumWriter[T] = new SpecificDatumWriter[T](schema)

  override def toBytes(record: T): Array[Byte] = {
    if (record == null) null
    else {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, NoBinaryEncoderReuse)
      writer.write(record, encoder)
      encoder.flush()
      out.close()
      out.toByteArray
    }
  }

}