/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

/**
  * An abstract class to represent metadatastore exception
  *
  * @param `type`     the type of the exception
  * @param reason     the reason for the exception
  * @param root_cause the root cause for the exception
  * @param caused_by  the cause of the exception
  */
abstract class MetadataStoreException(`type`: String,
                                      reason: String,
                                      root_cause: Option[String],
                                      caused_by: Option[String])
  extends Exception(s"${`type`} ${reason}", None.orNull) {
  def getType(): String = {
    `type`
  }

  def getReason(): String = {
    reason
  }
}
