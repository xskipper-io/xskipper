/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.status

import io.xskipper.index.Index
import io.xskipper.metadatastore.MetadataVersionStatus.MetadataVersionStatus

/**
  * Case class to represent the index status
  *
  * @param indexes the set of indexes defined on the dataset
  * @param numberOfIndexedObjects the number of indexed objects in the dataset
  * @param metadataStoreSpecificProperties a Map of metadatastore specific parameters
  * @param metadataVersionStatus the version status of the metadata
  */
case class IndexStatusResult(indexes: Seq[Index],
                             numberOfIndexedObjects: Long,
                             metadataStoreSpecificProperties: Map[String, String],
                             metadataVersionStatus: MetadataVersionStatus)

