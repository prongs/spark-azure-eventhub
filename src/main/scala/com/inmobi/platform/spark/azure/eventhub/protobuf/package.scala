package com.inmobi.platform.spark.azure.eventhub

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.runtime.universe

/**
  * Support for generating catalyst schemas for protobuf objects.
  */
package object protobuf {
  /** The mirror used to access types in the universe */
  def mirror: universe.Mirror = universe.runtimeMirror(Thread.currentThread().getContextClassLoader)

  import universe._

  case class Schema(dataType: DataType, nullable: Boolean)

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor[T <: AbstractMessage](clazz: Class[T]): Schema = {
    schemaFor(mirror.classSymbol(clazz).toType)
  }

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor[T: TypeTag]: Schema = {
    schemaFor(localTypeOf[T])
  }

  /**
    * Return the Scala Type for `T` in the current classloader mirror.
    *
    * Use this method instead of the convenience method `universe.typeOf`, which
    * assumes that all types can be found in the classloader that loaded scala-reflect classes.
    * That's not necessarily the case when running using Eclipse launchers or even
    * Sbt console or test (without `fork := true`).
    *
    * @see SPARK-5281
    */
  private def localTypeOf[T: TypeTag]: `Type` = typeTag[T].in(mirror).tpe

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): Schema = {
    tpe match {
      case t if t <:< localTypeOf[AbstractMessage] =>
        val clazz = mirror.runtimeClass(t).asInstanceOf[Class[AbstractMessage]]
        val descriptor = clazz.getMethod("getDescriptor").invoke(null).asInstanceOf[Descriptor]

        import collection.JavaConversions._
        Schema(StructType(descriptor.getFields.map(structFieldFor)), nullable = true)
      case other =>
        throw new UnsupportedOperationException(s"Schema for type $other is not supported")
    }
  }

  private def structFieldFor(fd: FieldDescriptor): StructField = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    val dataType = fd.getJavaType match {
      case INT => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case BOOLEAN => BooleanType
      case STRING => StringType
      case BYTE_STRING => BinaryType
      case ENUM => StringType
      case MESSAGE =>
        import collection.JavaConversions._
        StructType(fd.getMessageType.getFields.map(structFieldFor))
    }
    StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dataType, containsNull = false) else dataType,
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }

  def parserFor[T <: AbstractMessage](clazz: Class[T]): SerializableParser[T] = {
    new SerializableParser(clazz.getMethod("parser").invoke(null).asInstanceOf[AbstractParser[T]])
  }

  class SerializableParser[T <: MessageLite](@transient val parser: AbstractParser[T]) extends Serializable with KryoSerializable {
    override def read(kryo: Kryo, input: Input): Unit = ???

    override def write(kryo: Kryo, output: Output): Unit = ???
  }

  def messageToRow[A <: AbstractMessage](message: A): InternalRow = {
    import collection.JavaConversions._

    def toRowData(fd: FieldDescriptor, obj: AnyRef) = {
      fd.getJavaType match {
        case BYTE_STRING => obj.asInstanceOf[ByteString].toByteArray
        case ENUM => UTF8String.fromString(obj.asInstanceOf[EnumValueDescriptor].getName)
        case MESSAGE => messageToRow(obj.asInstanceOf[AbstractMessage])
        case STRING => UTF8String.fromString(obj.asInstanceOf[String])
        case _ => obj
      }
    }

    val fieldDescriptors = message.getDescriptorForType.getFields
    InternalRow(
      fieldDescriptors.map { fd =>
        val obj = message.getField(fd)
        if (fd.isRepeated) {
          new GenericArrayData(obj.asInstanceOf[java.util.List[Object]].map(toRowData(fd, _)))
        } else {
          toRowData(fd, obj)
        }
      }: _*
    )
  }
}
