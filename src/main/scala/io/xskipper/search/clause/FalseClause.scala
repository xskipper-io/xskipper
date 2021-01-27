/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.clause

// clause which evaluates always to false
case class FalseClause(col : String) extends Clause
