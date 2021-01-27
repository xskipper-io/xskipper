/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.ibm.metaindex.metadata.index.types.util

import com.ibm.metaindex.metadata.index.types.BloomFilterMetaData
import io.xskipper.index.metadata.{BloomFilterMetaData => NewBloomFilterMetaData}
import org.apache.spark.sql.functions._

object BloomFilterTransformer {

  val transformLegacyBloomFilter = udf((oldBf: BloomFilterMetaData) => {
    if (oldBf != null) {
      NewBloomFilterMetaData(oldBf.bloomFilter)
    } else {
      null
    }
  })

}
