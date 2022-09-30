/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.api

import io.xskipper.api.util.APITestUtils
import io.xskipper.implicits._
import io.xskipper.index.execution.IndexBuilder
import io.xskipper.metadatastore.MetadataStoreManagerType
import io.xskipper.testing.util.Utils._
import io.xskipper.testing.util.{LogTrackerBuilder, Utils}
import io.xskipper.{Xskipper, _}
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.{Configurator, LoggerConfig}
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import java.nio.file.Files


abstract class XskipperAPISuiteBase(val mdStore: MetadataStoreManagerType,
                                    override val datasourceV2: Boolean = false)
  extends FunSuite
    with BeforeAndAfterEach
    with XskipperProvider
    with Logging {

  // monitor skipped files
  val regexp = "(.*).*#.*--------> SKIPPED!".r


  def getJSONName(): String = {
    concatPaths(System.getProperty("user.dir"), "src/test/resources/test_config/APITests.json")
  }

  def getTestDescriptors(): Seq[APITestDescriptor] = {
    APITestUtils.parseAPITestDescriptors(getJSONName())
  }


  def runTests(): Unit = {
    getTestDescriptors().foreach(td => {
      td.inputFormats.foreach(format => {
        val tempDirs = td.inputDatasets.map(td => {
          val dir = Files.createTempDirectory("xskipper_test").toString
          dir.deleteOnExit()
          dir
        })

        test(s"${td.name}, format = $format") {
          logInfo(s"running with ${td}")
          runSkipTest(td.inputDatasets, tempDirs, format, td.query)
        }
      })
    })
  }

  def getIndexBuilder(xskipper: Xskipper, indexTypes: Seq[(String, Seq[String])]): IndexBuilder = {
    Utils.getIndexBuilder(xskipper, indexTypes)
  }

  def runSkipTest(datasetLocators: Seq[DatasetDescriptor],
                  tempDirs: Seq[String],
                  format: String,
                  query: String): Unit = {
    val tracker = LogTrackerBuilder.getRegexTracker(regexp)

    val reader = Utils.getDefaultReader(spark, format)
    val suffix = Utils.getDefaultSuffix(format)

    def runQueryAndVerifySkippedFiles(filesToSkip: Set[String]): Unit = {
      // first, run the query w/o skipping
      spark.disableXskipper()
      tracker.clearSet()
      tracker.startCollecting()
      datasetLocators.zip(tempDirs).foreach { case (descriptor: DatasetDescriptor, dir: String) =>
        val df = reader.load(dir)
        df.createOrReplaceTempView(descriptor.viewName)
      }

      val vanillaResDf = spark.sql(query)
      val vanillaRes = vanillaResDf.rdd.collect()
      tracker.stopCollecting()
      assertResult(Set())(tracker.getResultSet())
      tracker.clearSet()

      spark.enableXskipper()
      tracker.startCollecting()
      // now enable skipping and run the query again. re-read to avoid any cache hits
      datasetLocators.zip(tempDirs).foreach { case (descriptor: DatasetDescriptor, dir: String) =>
        val df = reader.load(dir)
        df.createOrReplaceTempView(descriptor.viewName)
      }
      val withSkippingResDf = spark.sql(query)
      val withSkippingRes = withSkippingResDf.rdd.collect()
      tracker.stopCollecting()

      // verify correct files were skipped
      assertResult(filesToSkip)(tracker.getResultSet())
      // verify we get the same result running w/ and w/o skipping
      assertResult(vanillaRes)(withSkippingRes)

      // TODO: FIX STATS ISSUE FOR V2!
      if (!datasourceV2) {
        // verify the stats report the correct number of skipped files
        val actualSkippedFilesNum = Utils.getNumSkippedFilesAndClear()
        assertResult(filesToSkip.size)(actualSkippedFilesNum)
      }

    }

    // copy input & build metadata for each dataset
    logInfo("Copying input datasets and collecting md")
    datasetLocators.zip(tempDirs).foreach {
      case (descriptor: DatasetDescriptor, dir: String) =>
        val origInputLoc = Utils.concatPaths(descriptor.inputLocation, format)
        logInfo(s"Copying $origInputLoc to $dir")
        FileUtils.copyDirectory(origInputLoc, dir, false)
        val xskipper = getXskipper(dir)
        assert(!xskipper.isIndexed())
        val buildRes = getIndexBuilder(xskipper, descriptor.indexTypes).build(reader)
        assert(Utils.isResDfValid(buildRes))
        assert(xskipper.isIndexed())
    }

    val filesToSkip: Set[String] = datasetLocators.zip(tempDirs)
      .flatMap {
        case (descriptor: DatasetDescriptor, dir: String) =>
          descriptor.expectedSkippedFiles.map(Utils.concatPaths(dir, _).concat(suffix))
      }.toSet
    runQueryAndVerifySkippedFiles(filesToSkip)

    // if at least 1 of the input datasets has an auxiliary input location
    // then we have a refresh test here, copy in the auxiliary objects
    // and refresh the metadata
    if (datasetLocators.exists(_.updatedInputLocation.isDefined)) {
      // refresh metadata for each dataset that has an auxiliary input location
      logInfo("Copying updated inputs and refreshing metadata")
      datasetLocators.zip(tempDirs)
        .filter(_._1.updatedInputLocation.isDefined)
        .foreach {
          case (descriptor: DatasetDescriptor, dir: String) =>
            val updatedInputLoc = Utils.concatPaths(descriptor.updatedInputLocation.get, format)
            FileUtils.copyDirectory(updatedInputLoc, dir, false)
            val xskipper = getXskipper(dir)
            val buildRes = xskipper.refreshIndex(reader)
            assert(Utils.isResDfValid(buildRes))
        }
      val newFilesToSkip = datasetLocators.zip(tempDirs).flatMap {
        case (descriptor: DatasetDescriptor, dir: String) =>
          // if the dataset has an auxiliary input location then the expected
          // skipped files is taken from the updated set, else - the "original" expected
          // skipped files is taken
          val files = descriptor.expectedSkippedFilesAfterUpdate match {
            case Some(files) => files
            case None => descriptor.expectedSkippedFiles
          }
          files.map(Utils.concatPaths(dir, _).concat(suffix))
      }.toSet
      runQueryAndVerifySkippedFiles(newFilesToSkip)
    }

  }

  override def beforeEach(): Unit = {
    Xskipper.reset(spark)
    // Reregister the metadata store default
    Registration.setActiveMetadataStoreManager(mdStore)
  }

  runTests()
}
