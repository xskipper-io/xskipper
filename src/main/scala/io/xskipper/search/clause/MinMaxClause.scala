/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.clause

import org.apache.spark.sql.catalyst.expressions._

sealed abstract class OperatorType
case object GT extends OperatorType // >
case object GTE extends OperatorType  // >=
case object LT extends OperatorType // <
case object LTE extends OperatorType // <=

/**
  * Represents an abstract minmax clause
  *
  * @param col the column on which the clause is applied
  * @param op the operator of the inequality
  * @param value the value to be used in the inequality
  * @param min true if the clause is on min value and false o.w
  */
case class MinMaxClause(col : String, op : OperatorType, value : Literal, min : Boolean )
  extends Clause {
  require(RowOrdering.isOrderable(value.dataType),
    s""""DataType ${value.dataType} is not Orderable!"""")
}
