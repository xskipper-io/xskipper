/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search

import io.xskipper.search.clause.{AndClause, Clause, OrClause}
import io.xskipper.search.expressions.{ExprUtils, MetadataWrappedExpression}
import io.xskipper.search.filters.MetadataFilter
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or}

object MetadataQueryBuilder {

  /**
    * Creates an abstract clause from a sequence of conjunctive catalyst after applying
    * a sequence of MetadataFilters
    *
    * @param dataFilters sequence of conjunctive catalyst expressions
    * @param filters sequence of MetadataFilter that will be applied on the wrappedExpression
    * @return the clause if it is possible to create one using the filters, otherwise None
    */
  def getClause(dataFilters : Seq[Expression], filters: Seq[MetadataFilter]): Option[Clause] = {
    if (dataFilters.isEmpty) return None
    val wrappedExpression = ExprUtils.catalystExprToWrappedExpr(dataFilters.reduce(And))
    // apply all filters
    filters.foreach(_.processWrappedExpression(wrappedExpression))
    // create abstract clause
    wrappedExprToClause(wrappedExpression)
  }

  /**
    * Converts a MetadataWrappedExpression to abstract clause
    *
    * @param wrappedExpr
    * @return
    */
  private def wrappedExprToClause(wrappedExpr: MetadataWrappedExpression): Option[Clause] = {
    wrappedExpr.expr match {
      case _: And =>
        // scalastyle:off line.size.limit
        val selfClause = Option(if (!wrappedExpr.getClauseSet().isEmpty) wrappedExpr.getClauseSet().reduce(AndClause) else null)
        // scalastyle:on line.size.limit
        val leftCondition = wrappedExprToClause(wrappedExpr.children(0))
        val rightCondition = wrappedExprToClause(wrappedExpr.children(1))
        val childrenConjuctionCondition =
          ExprUtils.applyAndOperator(leftCondition, rightCondition, AndClause)
        ExprUtils.applyAndOperator(childrenConjuctionCondition, selfClause, AndClause)

      case _: Or =>
        // scalastyle:off line.size.limit
        val selfClause = Option(if (!wrappedExpr.getClauseSet().isEmpty) wrappedExpr.getClauseSet().reduce(AndClause) else null)
        // scalastyle:on line.size.limit
        val leftCondition = wrappedExprToClause(wrappedExpr.children(0))
        val rightCondition = wrappedExprToClause(wrappedExpr.children(1))
        val childrenDisjunctionCondition =
          ExprUtils.applyOrOperator(leftCondition, rightCondition, OrClause)
        ExprUtils.applyAndOperator(childrenDisjunctionCondition, selfClause, AndClause)

      // No need for special case for Not since the clause for Not(a)
      // is not necessarily the Not(Clause(a))
      // In addition we won't have case of Not with And or Or inside
      // because spark normalize the predicates
      // scalastyle:off line.size.limit
      case _ => Option(if (!wrappedExpr.getClauseSet().isEmpty) wrappedExpr.getClauseSet().reduce(AndClause) else null)
      // scalastyle:on line.size.limit
    }
  }
}
