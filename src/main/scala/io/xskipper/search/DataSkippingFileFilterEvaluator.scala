/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search

import io.xskipper.metadatastore.MetadataStoreManager
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession


/**
  * this class overrides the base file filter, it implements the same interface,
  * but only collect parent statistics WITHOUT reading any data during execution
  */
class DataSkippingFileFilterEvaluator(tid: String, backend: MetadataStoreManager,
                                      sparkSession: SparkSession,
                                      params: Map[String, String] = Map.empty[String, String])
              extends DataSkippingFileFilter (tid, backend, sparkSession, params) {

  override def isRequired(fs: FileStatus): Boolean = {
    // run the original data skipping logic
    super.isRequired(fs)
    // return false anyway since this is an evaluation mode
    false
  }
}
