/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import java.nio.file.Files

import io.xskipper.metadatastore.test.TestMetadataStoreManager
import io.xskipper.testing.util.Utils
import io.xskipper.testing.util.Utils._
import io.xskipper.{Xskipper, XskipperException, XskipperProvider}
import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

abstract class IndexBuilderSuite(override val datasourceV2: Boolean = false)
  extends AnyFunSuite
    with XskipperProvider {

  override def getXskipper(uri: String): Xskipper = {
    new Xskipper(spark, uri, TestMetadataStoreManager)
  }

  // set spark log level to info for better debugging
  spark.sparkContext.setLogLevel("INFO")
  val baseDir: String = Utils.concatPaths(System.getProperty("user.dir"), "src/test/resources/")

  test("test BloomFilter build fails on timestamp columns") {
    val inputPath: String = Utils.concatPaths(baseDir, "input_datasets/all_data_types/parquet/")
    val xskipper = getXskipper(inputPath)

    // Make sure before running any query that there is no index for the dataset
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    try {
      val reader = spark.read.format("parquet")
      xskipper.indexBuilder().addBloomFilterIndex("timestampType").build(reader)
      fail()
    } catch {
      // Testing MetaIndexManagerException pattern matching
      case e: XskipperException =>
        e.printStackTrace()
        assert(!xskipper.isIndexed())
      case _: Throwable => fail()
    }
  }

  test("test Illegal column name fails index build") {
    val inputPath: String = Utils.concatPaths(baseDir, "input_datasets/invalid/parquet/")
    val reader = spark.read.format("parquet")
    val xskipper = getXskipper(inputPath)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    assertThrows[XskipperException] {
      xskipper.indexBuilder().addMinMaxIndex("temp.val").build(reader)
    }

    assertThrows[XskipperException] {
      xskipper.indexBuilder().addValueListIndex("address").build(reader)
    }
  }


  test("test invalid column name that appears due to schema evolution") {
    // creating temp directory
    val tempDir = Files.createTempDirectory("index_builder_suite").toString
    tempDir.deleteOnExit()
    val origInput = Utils.concatPaths(baseDir, "input_datasets/invalid_nodot/parquet/")
    // copying original files
    FileUtils.copyDirectory(origInput, tempDir)

    val reader = spark.read.format("parquet")
    val xskipper = getXskipper(tempDir)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    val ib = xskipper.indexBuilder().addValueListIndex("address")
    assert(Utils.isResDfValid(ib.build(reader)))

    // Refresh - adding file with new column having invalid name
    val changesLoc = Utils.concatPaths(baseDir, "input_datasets/invalid/parquet/")
    FileUtils.copyDirectory(changesLoc, tempDir)
    assertThrows[XskipperException] {
      ib.createOrRefreshExistingIndex(reader.load(tempDir),
        ib.indexes.toSeq, true)
    }
  }

  test("test indexing on a partition column") {
    val inputPath = Utils.concatPaths(baseDir, "input_datasets/vallist1/initial/parquet/")
    val reader = spark.read.format("parquet")
    val xskipper = getXskipper(inputPath)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // trying to build an index on a partition column - should throw an exception
    assertThrows[XskipperException] {
      xskipper.indexBuilder().addMinMaxIndex("date").build(reader)
    }
  }

  // make sure that indexing non partitioned hive table fails
  test("indexing on non partitioned hive table") {
    val inputPath = Utils.concatPaths(baseDir,
      "input_datasets/vallist1/initial/parquet/date=2018-05-21/")

    // drop table if it already exists
    spark.sql("drop table if exists test")
    val createTableSQL =
      s"""CREATE TABLE IF NOT EXISTS test
          USING PARQUET
          LOCATION '${inputPath}'
          OPTIONS(mergeSchema='true')"""
    spark.sql(createTableSQL)

    val xskipper = getXskipper(inputPath)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    assertThrows[XskipperException] {
      xskipper.indexBuilder().addMinMaxIndex("temp").build()
    }
  }

  // make sure attempts to create duplicate indexes fail
  test("duplicate index creation") {
    val inputPath = Utils.concatPaths(baseDir,
      "input_datasets/vallist1/initial/parquet/date=2018-05-21/")
    val df_reader = spark.read.format("parquet")
    val xskipper = getXskipper(inputPath)
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // Same index
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addMinMaxIndex("temp")
        .addMinMaxIndex("temp")
        .build(df_reader)
    }

    // Same type and same column, one encrypted one not
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addMinMaxIndex("temp")
        .addMinMaxIndex("temp", "k1")
        .build(df_reader)
    }

    // Same type same column, different false positive prob.
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addBloomFilterIndex("address")
        .addBloomFilterIndex("address", 0.123456)
        .build(df_reader)
    }


  }

  test("index operations on non existing table") {
    val xskipper = getXskipper("mydb.mytable")
    // creating index
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addMinMaxIndex("date")
        .addMinMaxIndex(("date"))
        .build()
    }

    // refreshing index
    assertThrows[XskipperException] {
      xskipper.refreshIndex()
    }

    // get index stats
    assertThrows[XskipperException] {
      xskipper.describeIndex()
    }
  }

  test("index operations on invalid table identifier") {
    val xskipper = getXskipper("cos://mybucket.service/location")
    // creating index
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addMinMaxIndex("date")
        .build()
    }

    // refreshing index
    assertThrows[XskipperException] {
      xskipper.refreshIndex()
    }

    // get index stats
    assertThrows[XskipperException] {
      xskipper.describeIndex()
    }
  }

  test("indexing a path with wildcards") {
    val xskipper = getXskipper("cos://mybucket.service/location/*")
    // indexing with glob paths
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addMinMaxIndex(("date"))
        .build(spark.read.format("parquet"))
    }
  }
}

class IndexBuilderSuiteV1 extends IndexBuilderSuite(false)

class IndexBuilderSuiteV2 extends IndexBuilderSuite(true)
