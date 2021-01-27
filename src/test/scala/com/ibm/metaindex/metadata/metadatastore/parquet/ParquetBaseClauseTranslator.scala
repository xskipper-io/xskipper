/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.ibm.metaindex.metadata.metadatastore.parquet

import com.ibm.metaindex.metadata.index.types.BloomFilterMetaData
import org.apache.spark.sql.types.MetadataTypeUDT

// Legacy support for old bloom filter metadata
object ParquetBaseClauseTranslator {
  // legacy UDT for bloom filter metadata
  class BloomFilterMetaDataTypeUDT extends MetadataTypeUDT[BloomFilterMetaData]
}
