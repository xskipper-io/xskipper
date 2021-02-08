/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.types

import io.xskipper.metadatastore.parquet.ParquetXskipperProvider
import io.xskipper.types.BloomFilterIndexTypeSupportBase

class BloomFilterIndexTypeSupportParquet(override val datasourceV2: Boolean = false)
  extends BloomFilterIndexTypeSupportBase(datasourceV2) with ParquetXskipperProvider {

}

class BloomFilterIndexTypeSupportParquetV1 extends BloomFilterIndexTypeSupportParquet(false)
