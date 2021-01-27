/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.metadatastore.parquet.types

import io.xskipper.metadatastore.parquet.ParquetXskipperProvider
import io.xskipper.types.NestedFieldsSuiteBase

abstract class NestedFieldsSuiteParquet(override val datasourceV2: Boolean = false)
  extends NestedFieldsSuiteBase(datasourceV2) with ParquetXskipperProvider {}

class NestedFieldsSuiteParquetV1 extends NestedFieldsSuiteParquet(false)

class NestedFieldsSuiteParquetV2 extends NestedFieldsSuiteParquet(true)
