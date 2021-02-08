/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.metadatastore.parquet

import io.xskipper.api.IllegalSchemaSuiteBase

abstract class IllegalSchemaSuiteParquet(override val datasourceV2: Boolean = false)
  extends IllegalSchemaSuiteBase(datasourceV2)
    with ParquetXskipperProvider {
}

class IllegalSchemaSuiteParquetV1 extends IllegalSchemaSuiteParquet(false)
