/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.filters

import io.xskipper.search.clause._
import io.xskipper.search.expressions.MetadataWrappedExpression
import org.apache.spark.sql.catalyst.expressions._

/**
  * represents a meta data filter that can process a wrapped expression and add the relevant clauses
  */
trait MetadataFilter{
  /**
    * Process a wrapped expression by traversing the expression tree and adding the relevant
    * clauses according to the filter
    *
    * @param wrappedExpr the wrapped expression to be processed
    */
  def processWrappedExpression(wrappedExpr : MetadataWrappedExpression) : Unit

}

abstract class BaseMetadataFilter extends MetadataFilter {
  /**
    * Process a wrapped expression by traversing the expression tree and adding the relevant
    * clauses according to the filter
    *
    * @param wrappedExpr the wrapped expression to be processed
    */
  override def processWrappedExpression(wrappedExpr: MetadataWrappedExpression): Unit = {
    wrappedExpr.expr match {
      case _@(_: And | _: Or) =>
        wrappedExpr.children.foreach(processWrappedExpression(_))
      case _ =>
        val query = getQueryContent(wrappedExpr)
        query match {
          case Some(queryInstance) => wrappedExpr.addClause(queryInstance)
          case _ =>
      }
    }

  }

  /**
    * Returns a clause representing the query contents associated with the expression
    *
    * @param wrappedExpr the expression to be
    */
  protected def getQueryContent(wrappedExpr: MetadataWrappedExpression): Option[Clause]

}
