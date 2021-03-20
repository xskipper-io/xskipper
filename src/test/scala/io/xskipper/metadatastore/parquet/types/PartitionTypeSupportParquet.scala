/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.types

import io.xskipper.metadatastore.parquet.ParquetXskipperProvider
import io.xskipper.types.PartitionTypeSupport

abstract class PartitionTypeSupportParquet(override val datasourceV2: Boolean = false)
  extends PartitionTypeSupport(datasourceV2) with ParquetXskipperProvider {}

class PartitionTypeSupportParquetParquetV1 extends PartitionTypeSupportParquet(false)

class PartitionTypeSupportParquetParquetV2 extends PartitionTypeSupportParquet(true)
