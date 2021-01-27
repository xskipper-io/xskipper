/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.versioning.util

import java.io.File

import io.xskipper.metadatastore.parquet.versioning.VersionTest
import io.xskipper.metadatastore.parquet.{Parquet, ParquetMetadataStoreConf, ParquetMetadataStoreManager}
import io.xskipper.testing.util.Utils
import io.xskipper.{Registration, Xskipper}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import org.apache.spark.sql.SparkSession

object MetadataGenerator {

  def main(args: Array[String]): Unit = {
    runMetadataGeneration()
  }

  def runMetadataGeneration(): Unit = {
    // the JSON file that holds details of all the tests
    val jsonName = VersionTestUtils.getJsonFileName()
    // parse the file
    val testDescriptors = VersionTestUtils.parseTestListFromJson(jsonName)

    // copy all input datasets into the destination.
    VersionTestUtils.copyInputDatasets(testDescriptors)

    val spark: SparkSession = SparkSession.builder()
      .appName("Xskipper Tests")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

    testDescriptors.foreach(generateMD(spark, _))
  }


  def getXskipper(spark: SparkSession,
                  testDescriptor: VersionTest,
                  testedVersion: Long): Xskipper = {
    val xskipper = new Xskipper(spark, testDescriptor.getFullDestPath(),
      ParquetMetadataStoreManager)

    val basePath = testDescriptor.getMetadataBasePath(testedVersion)

    val params = Map[String, String](
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY -> basePath,
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY
        -> ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION)
    xskipper.setParams(params)
    xskipper
  }

  def generateMD(spark: SparkSession, testDescriptor: VersionTest): Unit = {
    val version = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION.toInt
    Xskipper.reset(spark)
    Registration.setActiveMetadataStoreManager(Parquet)
    val xskipper = getXskipper(spark, testDescriptor, version)
    if (xskipper.isIndexed()) {
      throw new IllegalStateException(
        s"""Index already exists for
           | ${
          testDescriptor.getFullDestPath()
        }
            version ${
          version
        } """.stripMargin.replaceAll("\n", ""))
    }
    val ib = Utils.getIndexBuilder(xskipper, testDescriptor.indexTypes)
    // generate the metadata
    ib.build(testDescriptor.getDFReader(spark))
    // delete unnecessary .crc files
    val crcFiles = FileUtils.listFiles(
      new File(testDescriptor.getMetadataBasePath(version)),
      new WildcardFileFilter("*.crc"),
      TrueFileFilter.INSTANCE)
    crcFiles.forEach(f => f.delete())
  }
}
