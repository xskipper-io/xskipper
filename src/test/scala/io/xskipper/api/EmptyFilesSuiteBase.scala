/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.api

import io.xskipper.XskipperProvider
import io.xskipper.testing.util.Utils
import org.scalatest.FunSuite

abstract class EmptyFilesSuiteBase extends FunSuite
  with XskipperProvider {
  val baseDir = Utils.concatPaths(System.getProperty("user.dir"),
    "src/test/resources/input_datasets/empty_ds/")
  Seq("parquet", "orc").foreach(format =>
    test(s"testing empty files for format $format") {
      val inputPath = Utils.concatPaths(baseDir, format)
      val xskipper = getXskipper(inputPath)

      // Make sure before running any query that there is no index for the dataset
      if (xskipper.isIndexed()) {
        xskipper.dropIndex()
      }
      val reader = Utils.getDefaultReader(spark, format)
      try {
        xskipper.indexBuilder().addMinMaxIndex("salary").build(reader)
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          fail()
      }
    }
  )
}
