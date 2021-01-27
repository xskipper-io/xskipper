/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.clause

/**
  * Represents an abstract clause for a metadata Query on a file.
  * a few handy definitions:
  * Representation:
  * for a Clause c and a (boolean) expression  e, we say that c Represents e,
  * if the following holds:
  * for every file f, if there exists a row in f that meets e, than f meets c.
  */
trait Clause

case class AndClause(right: Clause, left : Clause) extends Clause

case class OrClause(right: Clause, left: Clause) extends Clause

/**
  * word of caution: THIS IS A SIMPLE NOT OPERATOR,
  * THE NEGATION IS SIMPLE - NOT THE ONE DEFINED IN THE TRAIT
  *
  * @param c the clause to negate
  */
case class NotClause(c : Clause) extends Clause
