package uk.co.appministry.akka.zk

import java.io._

case class ZkSerializerMarshallingError(val cause: Throwable) extends Exception(cause)

trait ZkSerializer {
  def deserialize(bytes: Array[Byte]): Any
  def serialize(serializable: Any): Array[Byte]
}

case class SimpleSerializer() extends ZkSerializer {

  override def deserialize(bytes: Array[Byte]): Any = {
    try {
      val inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))
      return inputStream.readObject()
    } catch {
      case e: Throwable => throw ZkSerializerMarshallingError(e)
    }
  }

  override def serialize(serializable: Any): Array[Byte] = {
    try {
      val byteArrayOutputStream = new ByteArrayOutputStream()
      val outputStream = new ObjectOutputStream(byteArrayOutputStream)
      outputStream.writeObject(serializable)
      outputStream.close()
      return byteArrayOutputStream.toByteArray
    } catch {
      case e: Throwable => throw ZkSerializerMarshallingError(e)
    }
  }

}