/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.api

import java.io.File
import java.nio.file.Files

import io.xskipper.implicits._
import io.xskipper.testing.util.Utils._
import io.xskipper.testing.util.{LogTrackerBuilder, Utils}
import io.xskipper.{XskipperProvider, _}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager}
import org.scalatest.FunSuite

abstract class IllegalSchemaSuiteBase(override val datasourceV2: Boolean = false)
  extends FunSuite
    with XskipperProvider {
  // monitor skipped files
  val regexp = "(.*).*#.*--------> SKIPPED!".r

  // set debug log level specifically for xskipper search package
  LogManager.getLogger("io.xskipper.search").setLevel(Level.DEBUG)

  val userDir = System.getProperty("user.dir")
  val goodInputPath = Utils.concatPaths(userDir,
    "src/test/resources/input_datasets/schema_validation/good_input/")
  val badInputPath = Utils.concatPaths(userDir,
    "src/test/resources/input_datasets/schema_validation/bad_input/")

  test("Test illegal schema through evolution") {
    val skippedFilesTracker = LogTrackerBuilder.getRegexTracker("skipped", regexp)
    val destDir = Files.createTempDirectory("xskipper_schema_validation").toString
    destDir.deleteOnExit()
    val destFile = new File(destDir)
    val destInputPath = Utils.concatPaths(destDir, "data/")
    val destInputFile = new File(destInputPath)

    // create initial data
    if (destFile.exists()) {
      FileUtils.deleteDirectory(destFile)
    }
    destFile.mkdirs()
    destInputFile.mkdirs()
    FileUtils.copyDirectory(new File(goodInputPath), destInputFile)
    val xskipper = getXskipper(destInputPath)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }
    xskipper.indexBuilder().addValueListIndex("a1.b").build(spark.read.format("parquet"))
    val df = spark.read.parquet(destInputPath)
    spark = spark.enableXskipper()
    df.createOrReplaceTempView("foo")
    skippedFilesTracker.stopCollecting()
    skippedFilesTracker.clearSet()
    skippedFilesTracker.startCollecting()
    val queryRes = spark.sql("select * from foo where a1.b = 'b2'")
    queryRes.collect()
    skippedFilesTracker.stopCollecting()
    val filesToSkip = Utils.getResultSet(destInputPath, "good_file.snappy.parquet")
    assertResult(filesToSkip)(
      skippedFilesTracker.getResultSet())
    // now copy in the bad file that will cause the illegal schema to appear
    FileUtils.copyDirectory(new File(badInputPath), destInputFile)
    // make sure invalid schema warning is logged
    val invalidSchemeRegex = "Schema is invalid for file:(.*), no skipping will be attempted".r
    val invalidSchemaTracker = LogTrackerBuilder.getRegexTracker("invalidSchemeRegex",
      invalidSchemeRegex)
    val df2 = spark.read.parquet(destInputPath)
    df2.createOrReplaceTempView("foo2")
    skippedFilesTracker.clearSet()
    skippedFilesTracker.startCollecting()
    invalidSchemaTracker.startCollecting()
    val queryRes2 = spark.sql("select * from foo2 where a1.b = 'b2'")
    queryRes2.collect()
    skippedFilesTracker.stopCollecting()
    invalidSchemaTracker.stopCollecting()
    assertResult(Set())(skippedFilesTracker.getResultSet())
    assertResult(Set(destInputPath))(
      invalidSchemaTracker.getResultSet())
  }

  test("Test REFRESH prevention on illegal schema") {
    val destDir = Files.createTempDirectory("xskipper_schema_validation").toString
    destDir.deleteOnExit()
    val destFile = new File(destDir)
    val destInputPath = Utils.concatPaths(destDir, "data/")
    val destInputFile = new File(destInputPath)

    // create initial data
    if (destFile.exists()) {
      FileUtils.deleteDirectory(destFile)
    }
    destFile.mkdirs()
    destInputFile.mkdirs()
    FileUtils.copyDirectory(new File(goodInputPath), destInputFile)
    val xskipper = getXskipper(destInputPath)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }
    assert(Utils.isResDfValid(
      xskipper.indexBuilder()
        .addValueListIndex("a1.b")
        .build(spark.read.format("parquet"))))
    // now copy in the bad file that will cause the illegal schema to appear
    FileUtils.copyDirectory(new File(badInputPath), destInputFile)
    // make sure invalid schema warning is logged
    assertThrows[XskipperException](xskipper.refreshIndex(spark.read.format("parquet")))

  }
}
