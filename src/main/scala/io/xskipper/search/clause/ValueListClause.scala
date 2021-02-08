/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.clause

import org.apache.spark.sql.catalyst.expressions.Literal

/**
  * Represents an abstract value list clause
  *
  * @param col the column on which the clause is applied
  * @param values array of literals of with values representing the value list from the query
  *
  *
  * @param negated  if true the clause is used to check whether the value list contains values which
  *                 are different from all of the values in the list (used for inequality checks)
  *                 if false the clause is used to check whether the value list metadata contains a
  *                 value in the list (used for equality checks)
  */
case class ValueListClause(col : String, values : Array[Literal], negated: Boolean) extends Clause
