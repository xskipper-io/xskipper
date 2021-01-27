/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.filters

import io.xskipper.search.clause.{BloomFilterClause, Clause, FalseClause}
import io.xskipper.search.expressions.MetadataWrappedExpression
import io.xskipper.utils.Utils
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
  * Represents a bloom filter filter in - used to add add [[BloomFilterClause]]-s
  * to [[MetadataWrappedExpression]]
  * @param col the column on which the filter is applied
  */
case class BloomFilterFilter(col : String) extends BaseMetadataFilter {
  /**
    * Returns a clause representing the query contents associated with the expression
    *
    * @param wrappedExpr the expression to be processed
    */
  override protected def getQueryContent(wrappedExpr: MetadataWrappedExpression): Option[Clause] = {

    def isValidExpr(exp: Expression): Boolean = {
      Utils.isExpressionValidForSelection(exp) &&
        Utils.getName(exp).equalsIgnoreCase(col)
    }
    val conditionVals : Option[Array[Literal]] = wrappedExpr.expr match {
      case In(expr: Expression, values) if isValidExpr(expr) &&
          values.forall(x => x.isInstanceOf[Literal] &&
          isValidDataType(x.asInstanceOf[Literal].dataType)) =>
                Some(values.map(_.asInstanceOf[Literal]).toArray)
      case InSet(expr : Expression, values) if isValidExpr(expr) =>
        // InSet contains the values as their scala types
        // and not Literal converting to scala and back to literal
        // to handle cases such as UT8String being use
        // for String which is internal only for Spark use
        val toScala = CatalystTypeConverters.createToScalaConverter(expr.dataType)
        Some(values.map(v => Literal.create(toScala(v))).toArray)
      case EqualTo(expr: Expression, v: Literal) if isValidExpr(expr)
        && isValidDataType(v.dataType) => Some(Array(v))
      case EqualTo(v: Literal, expr: Expression) if isValidExpr(expr)
          && isValidDataType(v.dataType) => Some(Array(v))
      case _ => None
    }

    conditionVals match {
      case Some(vals) =>
        if (vals.nonEmpty) {
          Some(BloomFilterClause(col, vals))
        } else {
          Some(FalseClause(col))
        }
      case _ => None
    }
  }

  // Return true for data types that are supported by the bloom filter
  private def isValidDataType(dataType: DataType) : Boolean = {
    dataType match {
      case ByteType => true
      case StringType => true
      case LongType => true
      case IntegerType => true
      case ShortType => true
      case _ => false
    }
  }
}
