/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.types

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.xskipper.index.metadata.BloomFilterMetaData
import com.ibm.metaindex.metadata.index.types.{BloomFilterMetaData => OldBloomFilterMetaData}
import io.xskipper.index.metadata.BloomFilterMetaData
import io.xskipper.metadatastore.parquet.ParquetBaseClauseTranslator.BloomFilterMetaDataTypeUDT

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * This class expose the UDT registration which is private in spark.
  * The UDT is serialized and saved using java serialization
  */
object ParquetMetadataStoreUDTRegistrator {
  // scalastyle:off line.size.limit
  private lazy val defaultParquetUDTs: mutable.Map[String, String] = mutable.Map(
    (classOf[BloomFilterMetaData].getName, classOf[BloomFilterMetaDataTypeUDT].getName),
    (classOf[OldBloomFilterMetaData].getName,
      "com.ibm.metaindex.metadata.metadatastore.parquet.ParquetBaseClauseTranslator$BloomFilterMetaDataTypeUDT"))
  // scalastyle:on line.size.limit

  private val udtMap: mutable.Map[String, String] = defaultParquetUDTs
  /**
    * Registers an UserDefinedType to an user class. If the user class is already registered
    * with another UserDefinedType, warning log message will be shown.
    * @param userClass the name of user class
    * @param udtClass the name of UserDefinedType class for the given userClass
    */
  def registerUDT(userClass: String, udtClass: String) : Unit = {
    udtMap += ((userClass, udtClass))
  }

  /**
    * Returns the Class of UserDefinedType for the name of a given user class.
    * @param userClass class name of user class
    * @return Option value of the Class object of UserDefinedType
    */
  def getUDTFor(userClass: String): UserDefinedType[_] = {
    UDTRegistration.getUDTFor(userClass).get.newInstance().asInstanceOf[UserDefinedType[_]]
  }

  /**
    * Register all of the UDTs
    */
  def registerAllUDTs(): Unit = {
    udtMap.foreach{case (userClass: String, udtClass: String) =>
      UDTRegistration.register(userClass, udtClass)}
  }
}

// A generic UDT class java/scala serialization
class MetadataTypeUDT[T >: Null](implicit ct: ClassTag[T]) extends UserDefinedType[T] {
  override def sqlType: DataType = BinaryType

  override def serialize(obj: T): Array[Byte] = {
    // java/scala serialization
    val bfOut = new ByteArrayOutputStream()
    val oostream = new ObjectOutputStream(bfOut)
    oostream.writeObject(obj)
    oostream.flush()
    oostream.close()
    bfOut.toByteArray
  }

  override def deserialize(metadataBinary: Any): T = {
    // java/scala serialization
    val bfIn = new ByteArrayInputStream(metadataBinary.asInstanceOf[Array[Byte]])
    val iostream = new ObjectInputStream(bfIn)
    val md = iostream.readObject().asInstanceOf[T]
    iostream.close()
    bfIn.close()
    md
  }

  override def userClass: Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
}
