/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.api.XskipperAPISuiteBase

abstract class XskipperAPISuiteParquet(override val datasourceV2: Boolean = false) extends
  XskipperAPISuiteBase(Parquet, datasourceV2) with ParquetXskipperProvider {

}

class XskipperAPISuiteParquetV1 extends XskipperAPISuiteParquet(false)

class XskipperAPISuiteParquetV2 extends XskipperAPISuiteParquet(true)
