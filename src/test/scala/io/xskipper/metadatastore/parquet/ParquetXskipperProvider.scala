/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import java.nio.file.Files

import io.xskipper.testing.util.Utils._
import io.xskipper.{Xskipper, XskipperProvider}


trait ParquetXskipperProvider extends XskipperProvider {

  // create the metadata in a temporary location.
  // note that we have a different input location for each
  // test - don't use the same location for a test twice,
  // if necessary - copy the input to a temp dir
  // for each one the input is copied to a temp directory so we won't
  // have 2 tests running on each other's md.
  val mdPath = Files.createTempDirectory("xskipper_test").toString
  mdPath.deleteOnExit()

  override def getXskipper(uri: String): Xskipper = {
    val xskipper = new Xskipper(spark, uri, ParquetMetadataStoreManager)
    val params = Map[String, String](
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY -> mdPath,
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY
        -> ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION)
    xskipper.setParams(params)
    xskipper
  }
}
