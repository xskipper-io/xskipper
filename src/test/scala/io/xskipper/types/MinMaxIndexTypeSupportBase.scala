/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.types

import io.xskipper._
import io.xskipper.implicits._
import io.xskipper.testing.util.Utils
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  *
  * Test suite to checks the type support for min/max index
  */
abstract class MinMaxIndexTypeSupportBase(override val datasourceV2: Boolean) extends FunSuite
  with XskipperProvider with Logging {
  val logger = Logger.getLogger(this.getClass.getSimpleName()).setLevel(Level.INFO)
  // set debug log level specifically for xskipper package
  LogManager.getLogger("io.xskipper").setLevel(Level.DEBUG)

  val userDir = System.getProperty("user.dir")
  val inputPath = Utils.concatPaths(userDir,
    "src/test/resources/input_datasets/type_support/all_data_types/")


  test(testName = "test min/max index type support - without parquet optimization") {
    TypeSupportUtils.setParquetMinMaxOptimization(false)
    testMinMax(false)
  }

  test(testName = "test min/max index type support - with parquet optimization") {
    TypeSupportUtils.setParquetMinMaxOptimization(true)
    testMinMax(false)
  }

  // test to check parquet min/max optimization when collecting metadata in parallel
  test(testName = "test min/max index type support - " +
    "with parquet optimization and only numerical columns") {
    TypeSupportUtils.setParquetMinMaxOptimization(true)
    testMinMax(true)
  }

  protected def testMinMax(onlyOptimizedNumerical: Boolean = false): Unit = {

    val sparkSession = spark
    import sparkSession.implicits._

    // index the dataset - using the table name
    val xskipper = getXskipper(inputPath)
    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add min/max index for all supported types
    var ib = xskipper.indexBuilder()
      .addMinMaxIndex("byteType")
      .addMinMaxIndex("shortType")
      .addMinMaxIndex("integerType")
      .addMinMaxIndex("longType")
      .addMinMaxIndex("floatType")
      .addMinMaxIndex("doubleType")
    // add the non optimized columns if needed
    if (!onlyOptimizedNumerical) {
      ib = ib.addMinMaxIndex("decimalType")
        .addMinMaxIndex("stringType")
        .addMinMaxIndex("binaryType")
        .addMinMaxIndex("booleanType")
        .addMinMaxIndex("timestampType")
        .addMinMaxIndex("dateType")
    }
    ib.build(spark.read.format("parquet"))

    assert(xskipper.isIndexed(), "Failed to index dataset")

    // enable filtering
    spark.enableXskipper()

    // set the expected skipped files
    val expectedSkippedFiles = Utils.getResultSet(inputPath, "c1.snappy.parquet")

    val df = spark.read.parquet(inputPath)
    df.createOrReplaceTempView("table")
    // execute queries and monitor skipped files

    // check all numerical types
    TypeSupportUtils.checkNumerical(spark, "byteType", "byte", ">", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "shortType", "short", ">", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "integerType", "integer", ">", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "longType", "long", ">", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "floatType", "float", ">", expectedSkippedFiles)
    TypeSupportUtils.checkNumerical(spark, "doubleType", "double", ">", expectedSkippedFiles)

    if (!onlyOptimizedNumerical) {
      TypeSupportUtils.checkNumerical(spark, "decimalType",
        "decimal(10,0)", ">", expectedSkippedFiles)

      // check string type

      logInfo("Checking skipping on stringType")
      TypeSupportUtils.checkQuery(spark.sql("select * from table where stringType > 'abc'"),
        expectedSkippedFiles)

      // check binary type
      logInfo("Checking skipping on binaryType")
      TypeSupportUtils.checkQuery(df.select("*").where(
        $"binaryType" > lit(Array[Byte](1.toByte, 2.toByte))), expectedSkippedFiles)

      // check boolean type
      logInfo("Checking skipping on booleanType")
      TypeSupportUtils.checkQuery(df.select("*").where(
        $"booleanType" < lit(true)), expectedSkippedFiles)

      // check timestamp type
      logInfo("Checking skipping on timestampType")
      TypeSupportUtils.checkQuery(spark.sql(
        "select * from table where timestampType > to_timestamp('2018-02-28T02:06:43.000+02:00')"),
        expectedSkippedFiles)

      // check date type
      logInfo("Checking skipping on timestampType")
      TypeSupportUtils.checkQuery(spark.sql(
        "select * from table where dateType > to_date('2018-02-27')"),
        expectedSkippedFiles)
    }
  }
}
