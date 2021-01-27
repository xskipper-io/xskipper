/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import java.util.Locale

import io.xskipper.XskipperException
import io.xskipper.index.metadata.{MetadataType, ValueListMetaData}
import io.xskipper.status.Status
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

object ValueListIndex extends IndexCompanion[ValueListIndex] {
  override def apply(params: Map[String, String],
                     keyMetadata: Option[String],
                     cols: Seq[String]): ValueListIndex = {
    if (cols.size != 1) {
      throw new XskipperException("ValueList Index takes exactly 1 Column!")
    }
    ValueListIndex(cols(0), keyMetadata)
  }
}
/**
  * Represents an abstract ValueList index
  * @param col the column on which the index is applied
  */
case class ValueListIndex(col : String, keyMetadata : Option[String] = None)
  extends Index(Map.empty, keyMetadata, col) {

  override def getName: String = "valuelist"

  override def getRowMetadata(row: Row) : Any = {
    row.get(row.fieldIndex(colsMap(col).name))
  }

  override def reduce(md1: MetadataType, md2: MetadataType): MetadataType = {
    (md1, md2) match {
      case (null, null) => null
      case (md1: MetadataType, null) => md1
      case (null, md2: MetadataType) => md2
      case (md1: MetadataType, md2: MetadataType) =>
        md1.asInstanceOf[ValueListMetaData].values ++= md2.asInstanceOf[ValueListMetaData].values
        md1
    }
  }

  override def reduce(accuMetadata: MetadataType, curr: Any): MetadataType = {
    (accuMetadata, curr) match {
      case (null, null) => metadata.ValueListMetaData(scala.collection.mutable.HashSet[Any]())
      case (accuMetadata: MetadataType, null) => accuMetadata
      case (null, curr: Any) =>
        val res = metadata.ValueListMetaData(scala.collection.mutable.HashSet[Any]())
        res.values += curr
        res
      case (accuMetadata: MetadataType, curr: Any) =>
        accuMetadata.asInstanceOf[ValueListMetaData].values += curr
        accuMetadata
    }
  }

  /**
    * Gets a [[DataFrame]] and checks whether it is valid for the index
    *
    * @param df the [[DataFrame]] to be checked
    * @param schemaMap a map containing column names (as appear in the object) and their data types
    *                  the key is the column name in lower case
    * @throws [[XskipperException]] if invalid index
    */
  override def isValid(df: DataFrame,
                       schemaMap: Map[String, (String, DataType)]): Unit = {
    // Supporting all non complex types
    if (!(schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[NumericType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[StringType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[BinaryType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[BooleanType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[DateType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[TimestampType]
      )) {
      throw new XskipperException(Status.VALUELIST_WRONG_TYPE)
    }
  }

  /**
    * @return the (full) name of the MetaDataType class used by this index
    */
  override def getMetaDataTypeClassName(): String = classOf[ValueListMetaData].getName
}
