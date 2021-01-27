/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.clause

import org.apache.spark.sql.catalyst.expressions.Literal

/**
  * Represents an abstract bloom filter clause
  *
  * @param col the column on which the clause is applied
  * @param values sequence of values representing the value list
  */
case class BloomFilterClause(col: String, values: Array[Literal]) extends Clause
