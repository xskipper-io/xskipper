/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.index.MinMaxIndex
import org.apache.log4j.{Level, Logger}
import org.scalatest.funsuite.AnyFunSuite

class ParquetUtilsSuite extends AnyFunSuite {

  val minMaxIndexes = Seq(
    "mycol",
    "my_col",
    "my.col",
    "my#col",
    "my$_col",
    "my$#._col").map(MinMaxIndex(_))

  test("Test single-column-index column name generation V1") {
    val expectedColNamesV1 = Seq(
      "mycol_minmax",
      "my_col_minmax",
      "my.col_minmax",
      "my#col_minmax",
      "my$_col_minmax",
      "my$#._col_minmax")

    minMaxIndexes.zip(expectedColNamesV1).foreach { case (idx, expectedColName) =>
      assertResult(expectedColName)(ParquetUtils.getColumnName(idx, 1L))
    }
  }

  test("Test single-column-index column name generation V2") {

    val expectedColNamesV2 = Seq(
      "mycol_minmax_5",
      "my_col_minmax_6",
      "my$#$col_minmax_8",
      "my##col_minmax_7",
      "my$_col_minmax_7",
      "my$##$#$_col_minmax_12")

    minMaxIndexes.zip(expectedColNamesV2).foreach { case (idx, expectedColName) =>
      assertResult(expectedColName)(ParquetUtils.getColumnName(idx, 2L))
    }

  }

  test("test exception thrown on wrong version number"){
    assertThrows[ParquetMetaDataStoreException]{
      ParquetUtils.getColumnName(MinMaxIndex("asdasd"), -1L)
    }

    val currVersion = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION
    assertThrows[ParquetMetaDataStoreException]{
      ParquetUtils.getColumnName(MinMaxIndex("asdasd"), currVersion + 1)
    }
  }
}

