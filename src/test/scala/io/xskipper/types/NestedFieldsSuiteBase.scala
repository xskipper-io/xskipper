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
import org.scalatest.funsuite.AnyFunSuite

/**
  * Test suite to check nested type support
  */
abstract class NestedFieldsSuiteBase(override val datasourceV2: Boolean) extends AnyFunSuite
  with XskipperProvider with Logging {
  Logger.getLogger(this.getClass.getSimpleName()).setLevel(Level.INFO)
  // set debug log level specifically for xskipper package
  LogManager.getLogger("io.xskipper").setLevel(Level.DEBUG)

  val basePath = System.getProperty("user.dir")
  val inputPath = Utils.concatPaths( basePath,
    "src/test/resources/input_datasets/employees_nested/parquet/")


  test(testName = "test min/max index type support - without parquet optimization") {
    TypeSupportUtils.setParquetMinMaxOptimization(false)
    testMinMax()
  }

  // test to check parquet min/max optimization when collecting metadata in parallel
  test(testName = "test min/max index type support - " +
    "with parquet optimization and only numerical columns") {
    TypeSupportUtils.setParquetMinMaxOptimization(true)
    testMinMax()
  }

  protected def testMinMax(): Unit = {
    // index the dataset - using the table name
    val xskipper = getXskipper(inputPath)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add min/max index for nested levels
    val ib = xskipper.indexBuilder()
      .addMinMaxIndex("salary")
      .addMinMaxIndex("items.serial")
      .addMinMaxIndex("items.complex.min")
    ib.build(spark.read.format("parquet"))

    assert(xskipper.isIndexed(), "Failed to index dataset")

    // enable filtering
    spark = spark.enableXskipper()

    // set the expected skipped files
    val expectedSkippedFiles = Utils.getResultSet(inputPath, "c1.snappy.parquet")

    val df = spark.read.parquet(inputPath)
    df.createOrReplaceTempView("nested")
    // execute queries and monitor skipped files

    logInfo(s"Checking skipping on two levels nested >")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE items.complex.min > 1"),
      expectedSkippedFiles)

    logInfo(s"Checking skipping on one level nested <")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE items.serial < 444"),
      expectedSkippedFiles)

    logInfo(s"Checking skipping on after nested <")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE salary < 7000"),
      expectedSkippedFiles)
  }

  test("Test ValueList with nesting") {
    // index the dataset
    val xskipper = getXskipper(inputPath)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add valuelist index for nested levels
    val ib = xskipper.indexBuilder()
      .addValueListIndex("name")
      .addValueListIndex("items.name")
      .addValueListIndex("items.complex.name")
    ib.build(spark.read.format("parquet"))

    assert(xskipper.isIndexed(), "Failed to index dataset")

    // enable filtering
    spark = spark.enableXskipper()

    // set the expected skipped files
    val expectedSkippedFiles = Utils.getResultSet(inputPath, "c1.snappy.parquet")

    val df = spark.read.parquet(inputPath)
    df.createOrReplaceTempView("nested")
    // execute queries and monitor skipped files

    logInfo(s"Checking skipping on two levels nested in")
    TypeSupportUtils.checkQuery(
      spark.sql("SELECT * FROM nested WHERE items.complex.name IN ('jus', 'mic')"),
      expectedSkippedFiles)
    logInfo(s"Checking skipping on one level nested ")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE items.name IN ('mac')"),
      expectedSkippedFiles)

    logInfo(s"Checking skipping on after nested")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE name IN ('Justin')"),
      expectedSkippedFiles)
  }

  test("Test BloomFilter with nesting") {
    val xskipper = getXskipper(inputPath)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add bloomfilter index for nested levels
    val ib = xskipper.indexBuilder()
      .addBloomFilterIndex("name")
      .addBloomFilterIndex("items.name")
      .addBloomFilterIndex("items.complex.name")
    ib.build(spark.read.format("parquet"))

    assert(xskipper.isIndexed(), "Failed to index dataset")

    // enable filtering
    spark = spark.enableXskipper()

    // set the expected skipped files
    val expectedSkippedFiles = Utils.getResultSet(inputPath, "c1.snappy.parquet")

    val df = spark.read.parquet(inputPath)
    df.createOrReplaceTempView("nested")
    // execute queries and monitor skipped files

    logInfo(s"Checking skipping on two levels nested in")
    TypeSupportUtils.checkQuery(
      spark.sql("SELECT * FROM nested WHERE items.complex.name IN ('jus', 'mic')"),
      expectedSkippedFiles)

    // check binary type
    logInfo(s"Checking skipping on one level nested <")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE items.name IN ('mac')"),
      expectedSkippedFiles)

    // check binary type
    logInfo(s"Checking skipping on after nested")
    TypeSupportUtils.checkQuery(spark.sql("SELECT * FROM nested WHERE name IN ('Justin')"),
      expectedSkippedFiles)
  }
}


