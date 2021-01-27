/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index.metadata

import scala.collection.mutable.HashSet

/**
  * ValueList meta data
  *
  * @param values Iterable containing the distinct strings that appear in the column
  */
@SerialVersionUID(1L)
case class ValueListMetaData(values: HashSet[Any]) extends MetadataType
