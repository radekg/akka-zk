package uk.co.appministry.akka.zk

import java.io._

/**
  * ZooKeeper data serialization error.
  * @param cause original error
  */
case class ZkSerializerMarshallingError(val cause: Throwable) extends Exception(cause)

/**
  * ZooKeeper data serializer.
  */
trait ZkSerializer {

  /**
    * Deserializer bytes to an object.
    * @param bytes byte array
    * @return an object
    */
  def deserialize(bytes: Array[Byte]): Any

  /**
    * Serialize an object to a byte array.
    * @param serializable an object
    * @return a byte array
    */
  def serialize(serializable: Any): Array[Byte]
}

/**
  * Simple ZooKeeper client serializer.
  */
case class SimpleSerializer() extends ZkSerializer {

  /**
    * Deserializer bytes to an object.
    * @param bytes byte array
    * @return an object
    */
  override def deserialize(bytes: Array[Byte]): Any = {
    try {
      val inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))
      return inputStream.readObject()
    } catch {
      case e: Throwable => throw ZkSerializerMarshallingError(e)
    }
  }

  /**
    * Serialize an object to a byte array.
    * @param serializable an object
    * @return a byte array
    */
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