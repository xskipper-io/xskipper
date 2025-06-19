/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import io.xskipper.utils.Utils
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class UtilsUnitTests extends AnyFunSuite {

  test("Test Illegal Schema") {
    val schema1 = StructType(Array(
      StructField("a", IntegerType),
      StructField("a1", StructType(Array(
        StructField("b.c", StringType),
        StructField("c", StringType)
      ))),
      StructField("a1.b", StructType(Array(
        StructField("b", StringType),
        StructField("c", StringType)
      )))
    ))

    assert(Utils.isSchemaValid(schema1) == false)

    val schema2 = StructType(Seq(StructField("a1.b", IntegerType)))
    assert(Utils.isSchemaValid(schema2) == false)
  }


  test("Test valid schema is not marked invalid") {
    val schema1 = StructType(Array(
      StructField("a", IntegerType),
      StructField("a1", StructType(Array(
        StructField("b", StringType),
        StructField("c", StringType)
      ))),
      StructField("a2", StructType(Array(
        StructField("b", StringType),
        StructField("c", StringType)
      )))
    ))

    assert(Utils.isSchemaValid(schema1))

    val schema2 = StructType(Seq(StructField("a1", IntegerType), StructField("a2", StringType)))
    assert(Utils.isSchemaValid(schema2))
  }

}
