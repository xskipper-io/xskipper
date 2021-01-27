/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.expressions

import io.xskipper.search.clause.Clause
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.mutable

/**
  * Represents a wrapped catalyst expression tree node in order to
  * allow storing clause set for each expression
  *
  * @param expr the catalyst expression to be wrapped
  * @param children the expressions's children
  */
case class MetadataWrappedExpression(expr : Expression, children : Seq[MetadataWrappedExpression]) {
  private val clauseSet : mutable.Set[Clause] = mutable.Set[Clause]()

  /**
    * Returns the clause set associated with this expression
    */
  def getClauseSet(): Set[Clause] = clauseSet.toSet

  /**
    * Adds an abstract clause to the clause set associated with this expression
    *
    * @param clause the clause to be added
    */
  def addClause(clause : Clause): Unit = {
    clauseSet += clause
  }
}
