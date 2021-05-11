/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

object MetadataVersionStatus extends Enumeration {
  type MetadataVersionStatus = Value

  /**
    * The stored metadata is from exactly the same version
    * as the metadata version of this jar.
    */
  val CURRENT = Value("current")

  /**
    * The stored metadata is from a version strictly smaller than
    * the metadata version of this jar. however, it can be used for filtering
    * and a REFRESH operation will result in the metadata having the `CURRENT` status
    */
  val DEPRECATED_SUPPORTED = Value("deprecated_supported")

  /**
    * The stored metadata is from a version strictly smaller than
    * the metadata version of this jar, a version so old it can't be used for filtering.
    * a REFRESH operation may or may not be able to upgrade it, up to the
    * metadata store's `isMetadataUpgradePossible` method.
    */
  val DEPRECATED_UNSUPPORTED = Value("deprecated_unsupported")

  /**
    * The stored metadata is from a version which is strictly greater
    * than the metadata version of this jar.
    * the metadata store is not expected to be able to either read or refresh this metadata.
    */
  val TOO_NEW = Value("too_new")
}
