/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.api.util

import io.xskipper.api.{APITestDescriptor, DatasetDescriptor}
import io.xskipper.testing.util.JSONImplicits._
import io.xskipper.testing.util.Utils._
import org.json.simple._
import org.json.simple.parser.JSONParser

object APITestUtils {

  val INPUT_REL_PATH: String = "src/test/resources"

  def getDefaultInputLocationPrefix(): String = {
    concatPaths(System.getProperty("user.dir"), INPUT_REL_PATH)
  }

  def parseAPITestDescriptors(fileName: String, inputLocationPrefix: String
  = getDefaultInputLocationPrefix()): Seq[APITestDescriptor] = {
    val input = scala.io.Source.fromFile(fileName).mkString
    val parser = new JSONParser()
    val jsonObject = parser.parse(input).asInstanceOf[JSONObject]
    parseAPITestDescriptors(jsonObject, inputLocationPrefix)
  }

  private def parseDatasetDescriptor(json: JSONObject, inputLocationPrefix: String)
  : DatasetDescriptor = {
    val inputLocation = concatPaths(inputLocationPrefix,
      json.get("inputLocation").asInstanceOf[String])
    val updatedInputLocation = json.getOpt[String]("updatedInputLocation") match {
      case Some(loc) => Some(concatPaths(inputLocationPrefix, loc))
      case None => None
    }
    val indexTypes = json.getSeq[String]("indexTypes").map(str => {
      val split = str.split(" ")
      assert(split.size == 2)
      (split.head, split.last.split(",").toSeq)
    })
    val expectedSkippedFiles = json.getSeq[String]("expectedSkippedFiles")
    val expectedSkippedFilesAfterUpdate = json
      .getOptionSeq[String]("expectedSkippedFilesAfterUpdate")
    (updatedInputLocation, expectedSkippedFilesAfterUpdate) match {
      case (Some(_), Some(_)) =>
      case (None, None) =>
      case _ => throw new IllegalArgumentException(
        "expectedSkippedFilesAfterUpdate and updatedInputLocation should be either both defined" +
          "or both undefined")
    }

    val viewName = json.getString("viewName")
    DatasetDescriptor(inputLocation,
      indexTypes,
      viewName,
      expectedSkippedFiles,
      updatedInputLocation,
      expectedSkippedFilesAfterUpdate)
  }

  def parseAPITestDescriptors(json: JSONObject,
                              inputLocationPrefix: String)
  : Seq[APITestDescriptor] = {
    val defaults = json.getAs[JSONObject]("defaults")
    val defaultInputFormats = defaults.getSeq[String]("inputFormats")

    val testObjs = json.getSeq[JSONObject]("tests")
    testObjs.map(testObject => {
      if (!testObject.containsKey("inputLocations")) {
        val x = 5
      }
      val inputDatasets = testObject
        .getSeq[JSONObject]("inputLocations")
        .map(parseDatasetDescriptor(_, inputLocationPrefix))
      APITestDescriptor(inputDatasets,
        testObject.getOptionSeq[String]("inputFormats").getOrElse(defaultInputFormats),
        testObject.getString("name"),
        testObject.getString("query")
      )
    })
  }
}
