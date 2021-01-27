/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.index.metadata.{MetadataType, MinMaxMetaData, ValueListMetaData}
import io.xskipper.index.{Index, MinMaxIndex, ValueListIndex}
import io.xskipper.metadatastore.MetadataStoreManagerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

object ParquetBaseMetaDataTranslator extends ParquetMetaDataTranslator {
  /**
    * Translates a [[MetadataType]] to the metadatastore representation
    *
    * @param metadataStoreManagerType the [[MetadataStoreManagerType]] to translate to
    * @param metadataType      the [[MetadataType]] to translate
    * @param index             the index that created the [[MetadataType]]
    * @return return type is Any since each metaDataStore may require different representation
    */
  override def translate(metadataStoreManagerType: MetadataStoreManagerType,
                         metadataType: MetadataType, index: Index): Option[Any] = {
    metadataStoreManagerType match {
      case Parquet =>
        metadataType match {
          case MinMaxMetaData(min, max) => Some(Row(min, max))
          case ValueListMetaData(values) => Some(values.toArray)
          case _ => None
        }
      case _ => None
    }
  }

  /**
    * Given an index returns the [[DataFrame]]
    * schema for the index [[MetadataType]]
    *
    * @param index
    */
  override def getDataType(index: Index): Option[DataType] = {
    index match {
      case _: MinMaxIndex =>
        Some(StructType(
          Seq(StructField("min", index.getIndexCols.head.dataType, true),
            StructField("max", index.getIndexCols.head.dataType, true))))
      case _: ValueListIndex =>
        Some(ArrayType(index.getIndexCols.head.dataType))
      case _ => None
    }
  }
}
