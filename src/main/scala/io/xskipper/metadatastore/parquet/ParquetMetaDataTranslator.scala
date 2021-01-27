/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.index.Index
import io.xskipper.metadatastore.MetaDataTranslator
import org.apache.spark.sql.types.DataType

trait ParquetMetaDataTranslator extends MetaDataTranslator {
  /**
    * Given an index returns the data type for the index [[MetaDataType]]
    * (if a translation is available)
    * @param index
    */
  def getDataType(index: Index): Option[DataType]
}
