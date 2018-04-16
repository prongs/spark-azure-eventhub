package com.inmobi.platform.spark.azure.eventhub

import java.net.URI
import java.nio.ByteBuffer

import com.google.protobuf.AbstractMessage
import com.inmobi.platform.spark.azure.eventhub.protobuf
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

class DefaultSource extends FileFormat with DataSourceRegister {
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    val className: Class[_ <: AbstractMessage] = Class.forName(options("proto.class.name")).asSubclass(classOf[AbstractMessage])
    Some(protobuf.schemaFor(className).dataType.asInstanceOf[StructType])
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = ???

  override def shortName(): String = "azure-eventhub"

  override def buildReader(
                            spark: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val protoClass: Class[_ <: AbstractMessage] = Class.forName(options("proto.class.name")).asSubclass(classOf[AbstractMessage])
    val parser: protobuf.SerializableParser[_ <: AbstractMessage] = protobuf.parserFor(protoClass)
    val broadCastedParser = spark.sparkContext.broadcast(parser)
    (file: PartitionedFile) => {
      val log = LoggerFactory.getLogger(classOf[DefaultSource])
      val parser = broadCastedParser.value.parser
      val schema = new Schema.Parser().parse(classOf[DefaultSource].getResourceAsStream("/eventhub.avsc"))
      val reader = {
        val in = new FsInput(new Path(new URI(file.filePath)), new Configuration())
        try {
          val datumReader = new GenericDatumReader[GenericRecord](schema)
          DataFileReader.openReader(in, datumReader)
        } catch {
          case NonFatal(e) =>
            log.error("Exception while opening DataFileReader", e)
            in.close()
            throw e
        }
      }

      // Ensure that the reader is closed even if the task fails or doesn't consume the entire
      // iterator of records.
      Option(TaskContext.get()).foreach { taskContext =>
        taskContext.addTaskCompletionListener { _ =>
          reader.close()
        }
      }

      reader.sync(file.start)
      val stop = file.start + file.length

      new Iterator[InternalRow] {

        private[this] var completed = false

        override def hasNext: Boolean = {
          if (completed) {
            false
          } else {
            val r = reader.hasNext && !reader.pastSync(stop)
            if (!r) {
              reader.close()
              completed = true
            }
            r
          }
        }

        override def next(): InternalRow = {
          if (reader.pastSync(stop)) {
            throw new NoSuchElementException("next on empty iterator")
          }
          val record: GenericRecord = reader.next()
          val bytes = record.get("Body").asInstanceOf[ByteBuffer]
          val message = parser.parseFrom(bytes)
          protobuf.messageToRow(message)
        }
      }

    }
  }
}
