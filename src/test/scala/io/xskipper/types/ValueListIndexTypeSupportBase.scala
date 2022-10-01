/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.types

import io.xskipper._
import io.xskipper.implicits._
import io.xskipper.testing.util.{LogTrackerBuilder, Utils}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Test suite to check the type support for value list index
  */
abstract class ValueListIndexTypeSupportBase(override val datasourceV2: Boolean)
  extends FunSuite with XskipperProvider with Logging {
  Logger.getLogger(this.getClass.getSimpleName()).setLevel(Level.INFO)

  // monitor skipped files
  val regexp = "(.*).*#.*--------> SKIPPED!".r
  val skippedFiles = LogTrackerBuilder.getRegexTracker("skipped", regexp)

  val userDir = System.getProperty("user.dir")
  val inputPath = Utils.concatPaths(userDir,
    "src/test/resources/input_datasets/type_support/all_data_types/")

  // set debug log level specifically for metaindex package
  LogManager.getLogger("io.xskipper").setLevel(Level.DEBUG)

  test(testName = "test value list index type support") {
    import spark.implicits._

    // index the dataset - using the table name
    val xskipper = getXskipper(inputPath)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add value list index for all supported types
    val indexBuildRes = xskipper.indexBuilder()
      .addValueListIndex("byteType")
      .addValueListIndex("shortType")
      .addValueListIndex("integerType")
      .addValueListIndex("longType")
      .addValueListIndex("floatType")
      .addValueListIndex("doubleType")
      .addValueListIndex("decimalType")
      .addValueListIndex("stringType")
      .addValueListIndex("binaryType")
      .addValueListIndex("booleanType")
      .addValueListIndex("timestampType")
      .addValueListIndex("dateType")
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
    TypeSupportUtils.checkNumerical(spark, "floatType", "float", "=", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "doubleType", "double", "=", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "decimalType", "decimal(10,0)",
      "=", expectedSkippedFiles)

    // check string type
    logInfo(s"Checking skipping on stringType")
    TypeSupportUtils.checkQuery(spark.sql("select * from table where stringType = 'abc'"),
      expectedSkippedFiles)

    // check binary type
    logInfo(s"Checking skipping on binaryType")
    TypeSupportUtils.checkQuery(df.select("*").where(
      $"binaryType" === lit(Array[Byte](1.toByte, 2.toByte))),
      expectedSkippedFiles)

    // check boolean type
    logInfo(s"Checking skipping on booleanType")
    TypeSupportUtils.checkQuery(df.select("*").where($"booleanType" === lit(true)),
      expectedSkippedFiles)

    // check timestamp type
    logInfo(s"Checking skipping on timestampType")
    TypeSupportUtils.checkQuery(spark.sql(
      "select * from table where timestampType = to_timestamp('2018-02-28T02:06:43.000+02:00')"),
      expectedSkippedFiles)

    // check date type
    logInfo(s"Checking skipping on dateType")
    TypeSupportUtils.checkQuery(spark.sql(
      "select * from table where dateType = to_date('2018-02-27')"),
      expectedSkippedFiles)
  }
}
