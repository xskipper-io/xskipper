/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.metadatastore.parquet.types

import io.xskipper.metadatastore.parquet.ParquetXskipperProvider
import io.xskipper.types.MinMaxIndexTypeSupportBase

abstract class MinMaxIndexTypeSupportParquet(override val datasourceV2: Boolean = false)
  extends MinMaxIndexTypeSupportBase(datasourceV2) with ParquetXskipperProvider {
}

class MinMaxIndexTypeSupportParquetV1 extends MinMaxIndexTypeSupportParquet(false)
