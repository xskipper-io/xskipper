/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index.metadata

import org.apache.spark.util.sketch.BloomFilter

/**
  * Bloom Filter metadata
  *
  * @param bloomFilter the bloom filter instance
  */
@SerialVersionUID(1L)
case class BloomFilterMetaData(bloomFilter: BloomFilter) extends MetadataType
