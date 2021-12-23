/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.ibm.metaindex.metadata.metadatastore.parquet

import com.ibm.metaindex.metadata.index.types.BloomFilterMetaData
import io.xskipper.metadatastore.parquet.SUIDAgnosticObjectInputStream
import org.apache.spark.sql.types.MetadataTypeUDT

import java.io.ByteArrayInputStream

// Legacy support for old bloom filter metadata
object ParquetBaseClauseTranslator {
  // legacy UDT for bloom filter metadata
  class BloomFilterMetaDataTypeUDT extends MetadataTypeUDT[BloomFilterMetaData] {
    override def deserialize(metadataBinary: Any): BloomFilterMetaData = {
      // java/scala serialization
      val wrapperIn = new ByteArrayInputStream(metadataBinary.asInstanceOf[Array[Byte]])
      val iostream = new SUIDAgnosticObjectInputStream(wrapperIn)
      val md = iostream.readObject().asInstanceOf[BloomFilterMetaData]
      iostream.close()
      wrapperIn.close()
      md
    }
  }
}
