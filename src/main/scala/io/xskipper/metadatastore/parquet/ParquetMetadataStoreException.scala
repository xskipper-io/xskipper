/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet


case class ParquetMetaDataStoreException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)
