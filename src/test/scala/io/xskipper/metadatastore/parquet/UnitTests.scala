/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.Xskipper
import io.xskipper.index.Index
import io.xskipper.testing.util.Utils
import io.xskipper.testing.util.Utils.{concatPaths, strToFile}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{TrueFileFilter, WildcardFileFilter}
import org.apache.hadoop.fs.FileStatus
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import java.io.File
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class UnitTests extends FunSuite
  with BeforeAndAfterEach
  with ParquetXskipperProvider {

  override val datasourceV2: Boolean = false

  override def beforeEach(): Unit = {
    Xskipper.reset(spark)
  }

  override def afterEach(): Unit = {
    Xskipper.reset(spark)
  }

  test("make sure indexExists do not throw exception") {
    // creating Xskipper without metadata configuration
    // to make sure metadata path resolving doesn't throw exception
    val xskipper = new Xskipper(spark, "/tmp", ParquetMetadataStoreManager)
    assert(!xskipper.isIndexed())
  }

  // NOTE - this test WILL FAIL when PME is available. the reason is it assumes PME is not loaded.
  test("make sure setting a footer key w/o PME loaded triggers an exception") {
    val metadatastore = new ParquetMetadataHandle(spark, "/tmp")
    val params = Map(
      "io.xskipper.parquet.encryption.footer.key" -> "k1")
    assertThrows[ParquetMetaDataStoreException] {
      metadatastore.setParams(params)
    }
  }

  // test creation of ParquetMetadataStore on path that doesn't contain parquet metadata (or empty)
  test("Check reading of invalid metadata") {
    // set metadata as some csv
    val baseDir: String = System.getProperty("user.dir")
    val input_path = Utils.concatPaths(baseDir,
      "src/test/resource/input_datasets/vallist1/initial/csv")

    val metadatastore = new ParquetMetadataHandle(spark, "/tmp")
    // set as explicit location
    metadatastore.setMDPath(input_path, "EXPLICIT_LOCATION")

    // make sure all operations that try to read the metadata fail with the right exception
    assertThrows[ParquetMetaDataStoreException] {
      metadatastore.removeMetaDataForFiles(Seq.empty)
    }

    assertResult(Seq.empty[Index])(metadatastore.getIndexes())

    assertThrows[ParquetMetaDataStoreException] {
      Await.result(metadatastore.getAllIndexedFiles(), Duration.Inf)
    }

    assertThrows[ParquetMetaDataStoreException] {
      metadatastore.removeMetaDataForFiles(Seq.empty)
    }

    assertThrows[ParquetMetaDataStoreException] {
      metadatastore.removeMetaDataForFiles(Seq.empty)
    }

    assertThrows[ParquetMetaDataStoreException] {
      metadatastore.getNumberOfIndexedObjects()
    }
  }

  test("check dedup of metadata with duplicate entries") {
    // create tmp directory
    val INPUT_REL_PATH: String = "src/test/resources"
    val dir = Files.createTempDirectory("xskipper_parquet_test").toString
    dir.deleteOnExit()

    val reader = spark.read.format("parquet")
    // copy the files and index a dataset
    val inputLocation = concatPaths(INPUT_REL_PATH,
      "input_datasets/vallist1/initial/parquet")
    FileUtils.copyDirectory(inputLocation, dir, false)

    val xskipper = getXskipper(dir)
    xskipper.indexBuilder()
      .addMinMaxIndex("temp")
      .build(reader)

    // duplicate the index values
    val metadatapath = xskipper.metadataHandle().asInstanceOf[ParquetMetadataHandle]
      .getMDPath().path.toString
    FileUtils.listFiles(
      new File(metadatapath),
      new WildcardFileFilter("*.parquet"),
      TrueFileFilter.INSTANCE).toArray.foreach { case f: File =>
      FileUtils.copyFile(f, concatPaths(metadatapath, s"${f.getName}_duplicate"))
    }

    val numIndexedObjectBeforeRefresh = {
      xskipper.metadataHandle().asInstanceOf[ParquetMetadataHandle].getNumberOfIndexedObjects()
    }
    assert(numIndexedObjectBeforeRefresh == 6,
      "number of indexed objects before refresh is invalid")

    // refresh will not remove the duplicates when the flag is set to false
    xskipper.setParams(Map(
      "io.xskipper.parquet.refresh.dedup" -> "false"))
    xskipper.refreshIndex(reader)

    val numIndexedObjectAfterRefreshFlagOff =
      xskipper.metadataHandle().asInstanceOf[ParquetMetadataHandle].getNumberOfIndexedObjects()
    assert(numIndexedObjectAfterRefreshFlagOff == 6,
      "number of indexed objects after refresh when dedup flag is of is invalid")

    // setting the flag to true and making sure duplicates are removed
    xskipper.setParams(Map(
      "io.xskipper.parquet.refresh.dedup" -> "true"))
    xskipper.refreshIndex(reader)

    val numIndexedObjectAfterRefreshFlagOn =
      xskipper.metadataHandle().asInstanceOf[ParquetMetadataHandle].getNumberOfIndexedObjects()
    assert(numIndexedObjectAfterRefreshFlagOn == 3,
      "number of indexed objects after refresh when dedup flag is on is invalid")
  }
}
