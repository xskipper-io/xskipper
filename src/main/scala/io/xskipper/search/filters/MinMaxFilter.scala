/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.filters

import io.xskipper.configuration.XskipperConf
import io.xskipper.search.clause._
import io.xskipper.search.expressions.MetadataWrappedExpression
import io.xskipper.utils.Utils
import org.apache.spark.sql.catalyst.expressions.{EqualTo, GreaterThan, GreaterThanOrEqual, In, InSet, LessThan, LessThanOrEqual, Literal, Not, _}

/**
  * Represents a minmax filter - used to add [[MinMaxClause]]-s to [[MetadataWrappedExpression]]
  *
  * @param col the column on which the filter is applied
  */
case class MinMaxFilter(col : String) extends BaseMetadataFilter {
  val IN_FILTER_THRESHOLD =
    XskipperConf.getConf(XskipperConf.XSKIPPER_MINMAX_IN_FILTER_THRESHOLD)

  /**
    * Returns a clause representing the query contents associated with the expression
    *
    * @param wrappedExpr the expression to be processed
    */
  override protected def getQueryContent(wrappedExpr: MetadataWrappedExpression): Option[Clause] = {
    def isValidExpr(expr: Expression): Boolean = {
      Utils.isExpressionValidForSelection(expr) && Utils.getName(expr).equalsIgnoreCase(col)
    }

    val res = wrappedExpr.expr match {
      case GreaterThan(expr: Expression, v: Literal) if isValidExpr(expr) =>
        MinMaxClause(col, GT, v, false)
      case GreaterThan(v: Literal, expr: Expression) if isValidExpr(expr) =>
        MinMaxClause(col, LT, v, true)
      case GreaterThanOrEqual(expr: Expression, v: Literal) if isValidExpr(expr) =>
        MinMaxClause(col, GTE, v, false)
      case GreaterThanOrEqual(v: Literal, expr: Expression) if isValidExpr(expr) =>
        MinMaxClause(col, LTE, v, true)
      case LessThan(expr: Expression, v: Literal) if isValidExpr(expr) =>
        MinMaxClause(col, LT, v, true)
      case LessThan(v: Literal, expr: Expression) if isValidExpr(expr) =>
        MinMaxClause(col, GT, v, false)
      case LessThanOrEqual(expr: Expression, v: Literal) if isValidExpr(expr) =>
        MinMaxClause(col, LTE, v, true)
      case LessThanOrEqual(v: Literal, expr: Expression) if isValidExpr(expr) =>
        MinMaxClause(col, GTE, v, false)
      case EqualTo(v: Literal, expr: Expression) if isValidExpr(expr) =>
        AndClause(MinMaxClause(col, LTE, v, true), MinMaxClause(col, GTE, v, false))
      case EqualTo(expr: Expression, v: Literal) if isValidExpr(expr) =>
        AndClause(MinMaxClause(col, LTE, v, true), MinMaxClause(col, GTE, v, false))
      case Not(EqualTo(expr: Expression, v: Literal)) if isValidExpr(expr) =>
        OrClause(MinMaxClause(col, GT, v, true), MinMaxClause(col, LT, v, false))
      case In(expr: Expression, values) if isValidExpr(expr) &&
        values.nonEmpty && values.forall(_.isInstanceOf[Literal]) =>
        if (values.size < IN_FILTER_THRESHOLD) {
            values.map(value => {
              val l = value.asInstanceOf[Literal]
              val v = Literal.create(l.value, l.dataType)
              AndClause(MinMaxClause(col, LTE, v, true), MinMaxClause(col, GTE, v, false))
            }).reduce(OrClause)
        } else {
          val head = values.head.asInstanceOf[Literal].value
          val (maxval, minval): (Any, Any) =
            values.foldLeft((head, head))((a, b) => {
              val bLiteral = b.asInstanceOf[Literal]
              (genericMax(a._1, bLiteral.value),
                genericMin(a._2, bLiteral.value))
            })
          val min = Literal.create(minval, expr.dataType)
          val max = Literal.create(maxval, expr.dataType)
          AndClause(MinMaxClause(col, GTE, min, false), MinMaxClause(col, LTE, max, true))
        }
      case In(expr: Expression, values) if isValidExpr(expr) &&
        values.isEmpty =>
        FalseClause(col)
      case InSet(expr: Expression, values) if isValidExpr(expr) =>
        if (values.size < IN_FILTER_THRESHOLD) {
          values.map(value => {
            val v = Literal.create(value, expr.dataType)
            AndClause(MinMaxClause(col, LTE, v, true), MinMaxClause(col, GTE, v, false))
          }).reduce(OrClause)
        } else {
          val (maxval, minval): (Any, Any) =
            values.foldLeft((values.head, values.head))((a, b) => {
              (genericMax(a._1, b),
                genericMin(a._2, b))
            })
          val min = Literal.create(minval, expr.dataType)
          val max = Literal.create(maxval, expr.dataType)
          AndClause(MinMaxClause(col, GTE, min, false), MinMaxClause(col, LTE, max, true))
        }
      case _ => null
    }
    Option(res)
  }

  private def genericMax[T <% Ordered[T]](e1: Any, e2: Any): T = {
    val ne1: T = e1.asInstanceOf[T]
    val ne2: T = e2.asInstanceOf[T]
    Ordering[T].max(ne1, ne2)
  }

  private def genericMin[T <% Ordered[T]](e1: Any, e2: Any): T = {
    val ne1: T = e1.asInstanceOf[T]
    val ne2: T = e2.asInstanceOf[T]
    Ordering[T].min(ne1, ne2)
  }
}
