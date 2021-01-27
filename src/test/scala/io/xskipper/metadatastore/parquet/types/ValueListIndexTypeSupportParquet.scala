/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.metadatastore.parquet.types

import io.xskipper.metadatastore.parquet.ParquetXskipperProvider
import io.xskipper.types.ValueListIndexTypeSupportBase

class ValueListIndexTypeSupportParquet(override val datasourceV2: Boolean = false)
  extends ValueListIndexTypeSupportBase(datasourceV2) with ParquetXskipperProvider {

}

class ValueListIndexTypeSupportParquetV1 extends ValueListIndexTypeSupportParquet(false)

class ValueListIndexTypeSupportParquetV2 extends ValueListIndexTypeSupportParquet(true)
