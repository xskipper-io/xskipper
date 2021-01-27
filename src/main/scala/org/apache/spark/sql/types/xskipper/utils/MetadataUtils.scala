/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.types.xskipper.utils

import org.apache.spark.sql.types.Metadata

/**
  * This class is used to expose package private val from org.apache.spark.sql.types
  */
object MetadataUtils {
  /**
    * Given [[Metadata]] returns the internal keys of all entries
    *
    * @param metadata the metadata to process
    * @return an iterable with all entries in the [[Metadata]]
    */
  def getKeys(metadata: Metadata): Iterable[String] = {
    metadata.map.keys
  }
}
