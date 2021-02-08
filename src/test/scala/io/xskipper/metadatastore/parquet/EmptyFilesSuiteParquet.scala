/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.api.EmptyFilesSuiteBase

abstract class EmptyFilesSuiteParquet(override val datasourceV2: Boolean = false)
  extends EmptyFilesSuiteBase with ParquetXskipperProvider {}

class EmptyFilesSuiteParquetV1 extends EmptyFilesSuiteParquet(false)
