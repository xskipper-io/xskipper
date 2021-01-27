/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

object XskipperException {
  def unapply(e: XskipperException): Option[(String, Throwable)] =
    Some((e.getMessage, e))}

class XskipperException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause) {
}
