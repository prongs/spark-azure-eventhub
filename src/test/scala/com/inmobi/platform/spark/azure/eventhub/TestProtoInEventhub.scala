package com.inmobi.platform.spark.azure.eventhub

import java.io.File
import java.nio.ByteBuffer
import java.util

import com.google.protobuf.AbstractMessage
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.inmobi.platform.Test.{Home, Person}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._

import scala.collection.JavaConversions._

class TestProtoInEventhub extends FlatSpec {

  lazy val spark: SparkSession = SparkSession.builder()
    //    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .master("local[*]").appName("test-proto-in-eventhub").getOrCreate()
  val schema: Schema = new Schema.Parser().parse(getClass.getResourceAsStream("/eventhub.avsc"))
  val person1 = Person.newBuilder.setName("person1").setAge(30)
  val person2 = Person.newBuilder.setName("person2").setAge(28)
  val protoObjects = List(Home.newBuilder.addPeople(person1).addPeople(person2).setAddress("Address").build())

  def compareMessage(message: AbstractMessage, row: Row): Unit = {
    message.getAllFields.foreach { case (field, value) =>
      val sparkValue = row.get(row.fieldIndex(field.getName))
      matchSparkAndProtoData(field, value, sparkValue)
    }

    def matchSparkAndProtoData(field: FieldDescriptor, value: Any, sparkValue: Any): Unit = {
      def compare(value: Any, sparkValue: Any): Unit = {
        field.getJavaType match {
          case MESSAGE =>
            compareMessage(value.asInstanceOf[AbstractMessage], sparkValue.asInstanceOf[Row])
          case ENUM =>
            assert(value.toString === sparkValue.toString)
          case _ =>
            assert(value == sparkValue)
        }
      }

      if (field.isRepeated) {
        value.asInstanceOf[util.List[Any]].zip(sparkValue.asInstanceOf[Seq[Any]]).foreach { case (a, b) =>
          compare(a, b)
        }
      } else {
        compare(value, sparkValue)
      }
    }
  }

  def readableProtoInEventhub(protoObject: AbstractMessage): Unit = {
    it should "be readable as it is after being written to eventhub" in {
      val data = {
        val data: GenericData.Record = new GenericData.Record(schema)
        data.put("SequenceNumber", 1L)
        data.put("Offset", "offsetValue")
        data.put("EnqueuedTimeUtc", System.currentTimeMillis().toString)
        val systemProps: util.Map[String, Any] = Map("path" -> "blah")
        data.put("SystemProperties", systemProps)
        data.put("Properties", systemProps)
        data.put("Body", ByteBuffer.wrap(protoObject.toByteArray))
        data
      }
      val file = File.createTempFile("eventinfo-proto-", ".avro")
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, file)
      dataFileWriter.append(data)
      dataFileWriter.close()

      val df = spark.read.option("proto.class.name", protoObject.getClass.getName).azureEventhub(file.getAbsolutePath)
      val rows = df.collect()
      assert(rows.length === 1)
      val row: Row = rows.head
      compareMessage(protoObject, row)
    }
  }

  protoObjects.foreach { protoObject =>
    it should behave like readableProtoInEventhub(protoObject)
  }
}