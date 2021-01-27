/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.types

import io.xskipper.XskipperProvider
import io.xskipper.implicits._
import io.xskipper.testing.util.Utils
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.internal.Logging
import org.scalatest.FunSuite

/**
  * Test suite to checks the type support for bloom list index
  */
abstract class BloomFilterIndexTypeSupportBase(override val datasourceV2: Boolean)
  extends FunSuite with XskipperProvider with Logging {
  val logger = Logger.getLogger(this.getClass.getSimpleName()).setLevel(Level.INFO)
  // set debug log level specifically for xskipper package
  LogManager.getLogger("io.xskipper").setLevel(Level.DEBUG)

  val basePath = System.getProperty("user.dir")
  val inputPath = Utils.concatPaths(basePath,
    "src/test/resources/input_datasets/type_support/all_data_types/")

  test(testName = "test bloom filter index type support") {
    // index the dataset - using the table name
    val xskipper = getXskipper(inputPath)
    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add bloom filter index for all supported types
    val indexBuildRes = xskipper.indexBuilder()
      // add bloom filter index for all supported types
      .addBloomFilterIndex("byteType")
      .addBloomFilterIndex("shortType")
      .addBloomFilterIndex("integerType")
      .addBloomFilterIndex("longType")
      .addBloomFilterIndex("stringType")
      .build(spark.read.format("parquet"))
    indexBuildRes.show()

    assert(xskipper.isIndexed(), "Failed to index dataset")

    // enable filtering
    spark.enableXskipper()

    // set the expected skipped files
    val expectedSkippedFiles = Utils.getResultSet(inputPath, "c0.snappy.parquet")

    val df = spark.read.parquet(inputPath)
    df.createOrReplaceTempView("table")

    // check all numerical types
    TypeSupportUtils.checkNumerical(spark, "byteType", "byte", "=", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "shortType", "short", "=", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "integerType", "integer", "=", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "longType", "long", "=", expectedSkippedFiles)

    // check string type
    logInfo(s"Checking skipping on stringType")
    TypeSupportUtils.checkQuery(
      spark.sql("select * from table where stringType = 'abc'"), expectedSkippedFiles)
  }
}
