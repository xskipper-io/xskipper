/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index.metadata

/**
  * MinMax meta data
  *
  * @param min the minimum value
  * @param max the maximum value
  */
@SerialVersionUID(1L)
case class MinMaxMetaData(var min: Any, var max: Any) extends MetadataType
