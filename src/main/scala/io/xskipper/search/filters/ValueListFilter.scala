/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.filters

import io.xskipper.search.clause.{Clause, FalseClause, ValueListClause}
import io.xskipper.search.expressions.MetadataWrappedExpression
import io.xskipper.utils.Utils
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ArrayType, DataType}

/**
  * Represents a value list filter in - used to add [[ValueListClause]]-s
  * to [[MetadataWrappedExpression]]
  *
  * @param col the column on which the filter is applied
  */
case class ValueListFilter(col : String) extends BaseMetadataFilter {
  /**
    * Returns a clause representing the query contents associated with the expression
    *
    * @param wrappedExpr the expression to be processed
    */
  override protected def getQueryContent(wrappedExpr: MetadataWrappedExpression): Option[Clause] = {
    val (expression, negate) = wrappedExpr.expr match {
      case Not(exp) => (exp, true)
      case e => (e, false)
    }

    def isValidExpr(exp: Expression): Boolean = {
      Utils.isExpressionValidForSelection(exp) && Utils.getName(exp).equalsIgnoreCase(col)
    }
    val conditionVals: Option[(Array[Any], DataType)] = expression match {
      case In(expr: Expression, values) if isValidExpr(expr)
        && values.forall(_.isInstanceOf[Literal]) =>
          Some(values.map(_.asInstanceOf[Literal].value).toArray, expr.dataType)
      case InSet(expr: Expression, values) if isValidExpr(expr) =>
        // InSet contains the values as their scala types and not Literal
        // converting to scala and back to literal to handle cases such as UT8String
        // being use for String which is internal only for Spark use
        Some(values.toArray, expr.dataType)
      case EqualTo(expr: Expression, v: Literal) if isValidExpr(expr) =>
        Some(Array(v.value), expr.dataType)
      case EqualTo(v: Literal, expr: Expression) if isValidExpr(expr) =>
        Some(Array(v.value), expr.dataType)
      case _ => None
    }

    conditionVals match {
      case Some((vals, dataType)) =>
        if (vals.nonEmpty) {
          val valueList = Literal.create(vals, ArrayType(dataType))
          Some(ValueListClause(col, valueList, negate))
        } else {
          Some(FalseClause(col))
        }
      case _ => None // the match entered the default... no query to return.
    }
  }
}
