/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.metadatastore.parquet.encryption

import io.xskipper.index.{BloomFilterIndex, MinMaxIndex, ValueListIndex}
import io.xskipper.metadatastore.parquet.ParquetMetadataStoreConf
import org.apache.spark.sql
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Values {

  val k1 = "iKwfmI5rDf7HwVBcqeNE6w== "
  val k2 = "LjxH/aXxMduX6IQcwQgOlw== "
  val k3 = "rnZHCxhUHr79Y6zvQnxSEQ=="
  val footerKeyLabel = "k1"
  val minMaxIdx = MinMaxIndex("temp", Some("k1"))
  val bloomFilterIdx = BloomFilterIndex("city", keyMetadata = Some("k1"))
  val valListIdx = ValueListIndex("city")

  val indexes = Seq(
    minMaxIdx,
    bloomFilterIdx,
    valListIdx
  )

  minMaxIdx.generateColsMap(Map("temp" -> ("temp", StructField("temp", IntegerType, true))))
  bloomFilterIdx.generateColsMap(Map("city" -> ("city", StructField("city", StringType, true))))
  valListIdx.generateColsMap(Map("city" -> ("city", StructField("city", StringType, true))))

  val dtField = StructField("dt", DateType, true)
  val yearField = StructField("year", StringType, true)
  val partitionSchema = StructType(Seq(dtField, yearField))

  val expectedDtField = StructField("virtual_dt", DateType, true)
  val expectedYearField = StructField("virtual_year", StringType, true)

  val tableIdentifier = "foo/bar"
  val columnKeyListStringEncryptedFooter =
    "k1:temp_minmax_4.min,temp_minmax_4.max,city_bloomfilter_4,obj_name,virtual_dt,virtual_year"

  val columnKeyListStringPlaintextFooter =
    "k1:temp_minmax_4.min,temp_minmax_4.max,city_bloomfilter_4,obj_name,virtual_dt,virtual_year"

  val encryptionMetaEncryptedFooter = new sql.types.MetadataBuilder()
    .putString(ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SHORT_KEY,
      columnKeyListStringEncryptedFooter)
    .putString(ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SHORT_KEY, footerKeyLabel)
    .build()

  val masterMetaEncryptedFooter = new sql.types.MetadataBuilder()
    .putMetadata("encryption", encryptionMetaEncryptedFooter)
    .putString("tableIdentifier", tableIdentifier)
    .putLong("version", ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION)
    .build()

  val encryptionMetaPlaintextFooter = new sql.types.MetadataBuilder()
    .putString(ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SHORT_KEY,
      columnKeyListStringPlaintextFooter)
    .putString(ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_SHORT_KEY, "true")
    .putString(ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SHORT_KEY, footerKeyLabel)
    .build()

  val masterMetaPlaintextFooter = new sql.types.MetadataBuilder()
    .putMetadata("encryption", encryptionMetaPlaintextFooter)
    .putString("tableIdentifier", tableIdentifier)
    .putLong("version", ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION)
    .build()
}
