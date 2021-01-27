/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import java.util.Locale

import io.xskipper.XskipperException
import io.xskipper.index.metadata.{MetadataType, MinMaxMetaData}
import io.xskipper.status.Status
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

object MinMaxIndex extends IndexCompanion[MinMaxIndex] {
  override def apply(params: Map[String, String],
                     keyMetadata: Option[String],
                     cols: Seq[String]): MinMaxIndex = {
    if (cols.size != 1) {
      throw new XskipperException("MinMax Index takes exactly 1 Column!")
    }
    MinMaxIndex(cols(0), keyMetadata)
  }
}
/**
  * Represents an abstract MinMax index
  * @param col the column on which the index is applied
  */
case class MinMaxIndex(col: String, keyMetadata: Option[String] = None)
  extends Index(Map.empty, keyMetadata, col) {

  override def getName : String = "minmax"
  /**
    * @return the (full) name of the MetaDataType class used by this index
    */
  override def getMetaDataTypeClassName(): String = classOf[MinMaxMetaData].getName

  override def getRowMetadata(row: Row): Any = {
    // The row contains an orderable data type in the requested column
    // as it was checked during index building
    row.getAs(colsMap(col).name)
  }

  /**
    * Returns max value of two Ordered objects (assuming both of the same datatype)
    *
    * @param e1 ordered object
    * @param e2 ordered object
    * @return max value of the given objects
    */
  private def genericMax[T <% Ordered[T]](e1: Any, e2: Any): T = {
    val ne1: T = e1.asInstanceOf[T]
    val ne2: T = e2.asInstanceOf[T]
    Ordering[T].max(ne1, ne2)
  }

  /**
    * Returns min value of two Ordered objects (assuming both of the same datatype)
    *
    * @param e1 ordered object
    * @param e2 ordered object
    * @return min value of the given objects
    */
  private def genericMin[T <% Ordered[T]](e1: Any, e2: Any): T = {
    val ne1: T = e1.asInstanceOf[T]
    val ne2: T = e2.asInstanceOf[T]
    Ordering[T].min(ne1, ne2)
  }

  override def reduce(md1: MetadataType, md2: MetadataType): MetadataType = {
    (md1, md2) match {
      case (null, null) => null
      case (md1: MetadataType, null) => md1
      case (null, md2: MetadataType) => md2
      case (md1: MetadataType, md2: MetadataType) =>
        // extract the min/max values
        val minmaxmd1 = md1.asInstanceOf[MinMaxMetaData]
        val minmaxmd2 = md2.asInstanceOf[MinMaxMetaData]

        // Update the metadata
        minmaxmd1.max = (minmaxmd1.max, minmaxmd2.max) match {
          case (null, null) => null
          case (null, y) => y
          case (x, null) => x
          case (x, y) => genericMax(x, y)
        }

        minmaxmd1.min = (minmaxmd1.min, minmaxmd2.min) match {
          case (null, null) => null
          case (null, y) => y
          case (x, null) => x
          case (x, y) => genericMin(x, y)
        }
        minmaxmd1
    }
  }

  override def reduce(accuMetadata: MetadataType, curr: Any): MetadataType = {
    (accuMetadata, curr) match {
      case (null, null) => MinMaxMetaData(null, null)
      case (accuMetadata: MetadataType, null) => accuMetadata
      case (null, curr: Any) =>
        MinMaxMetaData(curr , curr)
      case (accuMetadata: MetadataType, curr: Any) =>
        val minmaxmd = accuMetadata.asInstanceOf[MinMaxMetaData]
        minmaxmd.max = minmaxmd.max match {
          case null => curr
          case x => genericMax(x, curr)
        }
        minmaxmd.min = minmaxmd.min match {
          case null => curr
          case x => genericMin(x, curr)
        }
        minmaxmd
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
      throw new XskipperException(Status.MINMAX_WRONG_TYPE)
    }
  }
}
