/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.versioning

import io.xskipper.implicits._
import io.xskipper.metadatastore.parquet.versioning.util.VersionTestUtils
import io.xskipper.metadatastore.parquet.{Parquet, ParquetMetadataStoreConf, ParquetMetadataStoreManager}
import io.xskipper.testing.util.{LogTracker, LogTrackerBuilder}
import io.xskipper.{Registration, Xskipper, XskipperProvider}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.util.matching.Regex

class VersionTestSuite(override val datasourceV2: Boolean) extends FunSuite
  with BeforeAndAfterEach
  with XskipperProvider
  with Logging {

  override def getXskipper(uri: String): Xskipper = {
    new Xskipper(spark, uri, ParquetMetadataStoreManager)
  }

  // the JSON file that holds details of all the tests
  val jsonName = VersionTestUtils.getJsonFileName()
  // parse the file
  val testDescriptors = VersionTestUtils.parseTestListFromJson(jsonName)

  LogManager.getLogger("io.xskipper.search.DataSkippingFileFilter").setLevel(Level.TRACE)
  val allowedFormats = Set("csv", "json", "parquet", "orc")

  val skippableFilesRegex: Regex = "(.*).*#.*-->SKIPPABLE!".r
  val skippedFilesRegex = "(.*).*#.*--------> SKIPPED!".r

  // copy all input datasets into the destination. we can't use the original
  // as the metadata already exists and its tableIdentifier and file names are already written.
  // notice that for this reason we are also unable to simply run the queries and expect
  // any skipping, as the fileIDs will be different (timestamps), so we use a LogTracker
  // to extract the skippable files.
  VersionTestUtils.copyInputDatasets(testDescriptors)

  // copy the metadata for the refresh tests
  VersionTestUtils.copyInputMetadata(testDescriptors,
    ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION)


  def runTests(): Unit = {
    testDescriptors.foreach(td =>
      (td.minVersion to td.maxVersion).foreach(version => {
        // testing "regular" md usage
        runSkipTest(td, version)
        // testing REFRESH
        runRefreshTest(td, version)
      }
      )
    )
  }


  def getXskipper(testDescriptor: VersionTest,
                  testedVersion: Long, refreshTest: Boolean = false): Xskipper = {
    val xskipper = getXskipper(testDescriptor.getFullDestPath())

    val basePath = refreshTest match {
      case true => testDescriptor.getTargetMetadataBasePath(testedVersion)
      case false => testDescriptor.getMetadataBasePath(testedVersion)
    }
    val params = Map[String, String](
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY -> basePath,
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY
        -> ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION)
    xskipper.setParams(params)
    xskipper
  }

  def runSkipTest(testDescriptor: VersionTest, testedVersion: Long): Unit = {
    // not part of the test, failure here implies an error in the test configuration.
    assert(allowedFormats.contains(testDescriptor.inputFormat))
    test(
      s"""${testDescriptor.name}
     VERSION=${testedVersion} testID=${testDescriptor.testId}""") {
      // get the index manager
      val xskipper = getXskipper(testDescriptor, testedVersion, false)
      assert(xskipper.isIndexed())
      // inject the data skipping rule
      spark.enableXskipper()
      // our md won't contain an entry for the files we have in the FS
      // (the timestamp is different) so we check using the SKIPPABLE regex
      // as the files won't be skipped.
      val skippableFilesTracker: LogTracker[String] =
      LogTrackerBuilder.getRegexTracker(skippableFilesRegex)
      skippableFilesTracker.startCollecting()
      // read the input dataset & run the queries
      val df = testDescriptor.getDFReader(spark).load(testDescriptor.getFullDestPath())
      df.createOrReplaceTempView("sample")
      val res = spark.sql(testDescriptor.query)
      res.show()
      skippableFilesTracker.stopCollecting()
      val actualSkippableFiles = skippableFilesTracker.getResultSet()
      val fullDestPrefix = testDescriptor.getFullDestPath() + "/"
      val expectedSkippedFilePaths = testDescriptor.expectedSkippedFiles
        .map(fname => fullDestPrefix + fname).toSet
      assertResult(expectedSkippedFilePaths)(actualSkippableFiles)
    }
  }

  def runRefreshTest(testDescriptor: VersionTest, testedVersion: Long): Unit = {
    test(
      s"""REFRESH ${testDescriptor.name}
     VERSION=${testedVersion} testID=${testDescriptor.testId}""") {
      val xskipper = getXskipper(testDescriptor, testedVersion, true)
      assert(xskipper.isIndexed())
      val reader = testDescriptor.getDFReader(spark)
      xskipper.refreshIndex(reader)
      // inject the data skipping rule
      spark.enableXskipper()
      // we've just refreshed the md - it should actually contain
      // an entry for our files, we should expect to see them skipped!
      val skippedFilesTracker: LogTracker[String] =
      LogTrackerBuilder.getRegexTracker(skippedFilesRegex)
      skippedFilesTracker.startCollecting()
      // read the input dataset & run the queries
      val df = reader.load(testDescriptor.getFullDestPath())
      df.createOrReplaceTempView("sample")
      val res = spark.sql(testDescriptor.query).rdd.collect()
      skippedFilesTracker.stopCollecting()
      val actualSkippedFiles = skippedFilesTracker.getResultSet()
      val fullDestPrefix = testDescriptor.getFullDestPath() + "/"
      val expectedSkippedFilePaths = testDescriptor.expectedSkippedFilesAfterUpdate
        .map(fname => fullDestPrefix + fname).toSet
      assertResult(expectedSkippedFilePaths)(actualSkippedFiles)
    }
  }

  override def beforeEach(): Unit = {
    // Re-register the metadata store default
    Registration.setActiveMetadataStoreManager(Parquet)
  }

  override def afterEach(): Unit = {
    Xskipper.reset(spark)
  }

  runTests()
}

class VersionTestSuiteV1 extends VersionTestSuite(false)

class VersionTestSuiteV2 extends VersionTestSuite(true)
