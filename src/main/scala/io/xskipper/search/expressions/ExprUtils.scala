/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.expressions

import org.apache.spark.sql.catalyst.expressions.Expression

object ExprUtils {
  /**
    * Converts a catalyst expression to a wrapped expression (recursively)
    *
    * @param expr the catalyst expression to be converted
    * @return the resulting wrapped expression
    * @note It's assumed the boolean-operator prefix of the expression tree
    *       (that is, from the root down, stopping on non-boolean-operator
    *       nodes) is in NNF Form. this is extremely important, since
    *       the variant of GenClause and MergeClause used here fail when
    *       the NNF condition is not met (simple example (NOT(NOT(X < 5)).
    *
    */
  def catalystExprToWrappedExpr(expr: Expression): MetadataWrappedExpression = {
    val newChildren: Seq[MetadataWrappedExpression] =
      expr.children.map(child => catalystExprToWrappedExpr(child))
    MetadataWrappedExpression(expr, newChildren)
  }

  /**
    * Apply an and operator on Optional parameters, in case one of them is None
    * the non None parameter is returned
    *
    * @param f the operator to be applied
    */
  def applyAndOperator[T](a: Option[T], b: Option[T], f: (T, T) => T): Option[T] = {
    (a, b) match {
      case (Some(x), Some(y)) => Some(f(x, y))
      case (None, Some(y)) => Some(y) // same as b
      case (Some(x), None) => Some(x) // same as a
      case (None, None) => None
    }
  }

  /**
    * Apply an or operator on Optional parameters, the operator is applied only
    * if both parameters are defined
    *
    * @param f the operator to be applied
    */
  def applyOrOperator[T](a: Option[T], b: Option[T], f: (T, T) => T): Option[T] = {
    (a, b) match {
      case (Some(x), Some(y)) => Some(f(x, y))
      case _ => None
    }
  }

}
