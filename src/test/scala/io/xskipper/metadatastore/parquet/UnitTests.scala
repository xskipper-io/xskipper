/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.Xskipper
import io.xskipper.index.Index
import io.xskipper.testing.util.Utils
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class UnitTests extends FunSuite with BeforeAndAfterEach {

  val spark = SparkSession
    .builder()
    .appName("Parquet UnitTests")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .enableHiveSupport()
    .getOrCreate()

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
}
