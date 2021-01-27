/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.versioning.util

import java.io.File

import io.xskipper.metadatastore.parquet.versioning.VersionTest
import io.xskipper.testing.util.JSONImplicits._
import io.xskipper.testing.util.Utils
import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

object VersionTestUtils extends Logging {

  val INPUT_REL_PATH = "/src/test/resources/"

  def getJsonFileName(): String = {
    Utils.concatPaths(System.getProperty("user.dir"),
      "/src/test/resources/test_config/ParquetVersionTests.json")
  }

  /**
    * reads a JSON file and parses it into a sequence of [[VersionTest]].
    * The functions assumes that the JSON has the correct structure (e.g. keys and types)
    *
    * @param jsonFileName
    * @return
    */
  def parseTestListFromJson(jsonFileName: String): Seq[VersionTest] = {
    val jsonString = scala.io.Source.fromFile(jsonFileName).mkString
    val jsonObj = new JSONParser().parse(jsonString).asInstanceOf[JSONObject]
    val paramsObj = jsonObj.getAs[JSONObject]("params")
    val globalDestPathPrefix = paramsObj.getString("destPathPrefix")
    val globalMdLocationPrefix = paramsObj.getString("mdLocationPrefix")
    val globalMinVersion = paramsObj.getLong("minVersion")
    val globalTargetMdLocationPrefix = paramsObj.getString("targetMdLocationPrefix")

    val versionTestsSeq = jsonObj.getSeq[JSONObject]("versionTests")

    versionTestsSeq.map(obj => {
      val json = obj.getAs[JSONObject]("testDescription")
      val expectedSkippedFiles = json.getOptionSeq[String]("expectedSkippedFiles")
        .getOrElse(Seq())

      val indexTypes = json.getSeq[String]("indexTypes").map(str => {
        val split = str.split(" ")
        assert(split.size == 2)
        (split.head, split.last.split(",").toSeq)
      })

      VersionTest(name = json.getString("name"),
        inputFormat = json.getString("inputFormat"),
        dataSetOrigPath = json.getString("dataSetPath"),
        dataSetDestPathPrefix = json.getOrElse("destPathPrefix", globalDestPathPrefix),
        mdLocationPrefix = json.getOrElse("mdLocationPrefix", globalMdLocationPrefix),
        targetMdLocationPrefix = json.getOrElse(
          "targetMdLocationPrefix", globalTargetMdLocationPrefix),
        minVersion = json.getOrElse("minVersion", globalMinVersion.toLong),
        testId = json.getLong("testId"),
        indexTypes = indexTypes,
        query = json.getString("query"),
        expectedSkippedFiles = expectedSkippedFiles,
        expectedSkippedFilesAfterUpdate = json
          .getOptionSeq[String]("expectedSkippedFilesAfterUpdate").getOrElse(expectedSkippedFiles)
      )
    })
  }

  def copyInputDatasets(testDescriptors: Seq[VersionTest],
                        baseDir: String
                        = Utils.concatPaths(System.getProperty("user.dir"),
                          INPUT_REL_PATH)): Unit = {
    val origDestSeq: Seq[(String, String)] = testDescriptors.map(td =>
      (Utils.concatPaths(baseDir, td.dataSetOrigPath), td.getFullDestPath())).distinct
    // verify no directory appears twice as destination
    // (this will imply different origins since the sequence is unique)
    assert(origDestSeq.map(_._2).toSet.size == origDestSeq.size)
    origDestSeq.foreach({ case (orig: String, dest: String) =>
      val origFile = new File(orig)
      val destFile = new File(dest)
      if (destFile.exists()) {
        FileUtils.deleteDirectory(destFile)
      }
      destFile.mkdirs()
      FileUtils.copyDirectory(origFile, destFile, false)
    })
  }

  def copyInputMetadata(testDescriptors: Seq[VersionTest], maxVersion: Long): Unit = {
    testDescriptors.foreach(testDescriptor => {
      (testDescriptor.minVersion until maxVersion + 1).foreach(version => {
        val origFile = new File(testDescriptor.getMetadataBasePath(version))
        val destFile = new File(testDescriptor.getTargetMetadataBasePath(version))
        if (destFile.exists()) {
          FileUtils.deleteDirectory(destFile)
        }
        destFile.mkdirs()
        FileUtils.copyDirectory(origFile, destFile, false)
      })
    })
  }


}

class VersionTestException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause) {
}
