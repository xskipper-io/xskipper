/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.types

import io.xskipper.Xskipper
import io.xskipper.configuration.XskipperConf
import io.xskipper.testing.util.LogTrackerBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Utils class for index type support tests
  */
object TypeSupportUtils extends Logging {
  // monitor skipped files
  val regexp = "(.*).*#.*--------> SKIPPED!".r
  val skippedFiles = LogTrackerBuilder.getRegexTracker(regexp)

  /**
    * Checks skipping for a given dataframe by calling show and the dataframe
    * and monitoring the skipped files
    *
    * @param df                   the dataframe
    * @param expectedSkippedFiles the expected files to be skipped when
    *                             calling show on the dataframe
    */
  def checkQuery(df: DataFrame, expectedSkippedFiles: Set[String]): Unit = {
    skippedFiles.clearSet()
    skippedFiles.startCollecting()

    df.show(false)

    skippedFiles.stopCollecting()

    // assert skipping was successful
    assert(skippedFiles.getResultSet().equals(expectedSkippedFiles),
      s"Skipped files: ${skippedFiles.getResultSet().toString} did not match " +
        s"expectedSkippedFiles: ${expectedSkippedFiles.toString()}")
  }

  /**
    * Checks skipping on a numerical type
    *
    * @param sparkSession         spark session instance
    * @param colName              the column name to ask on
    * @param t                    the type name to be used in cast
    * @param operator             the operator to be used in the query
    * @param expectedSkippedFiles the set of expected files to be skipped
    */
  def checkNumerical(sparkSession: SparkSession, colName: String,
                     t: String, operator: String, expectedSkippedFiles: Set[String]): Unit = {
    logInfo(s"Checking skipping on ${colName}")
    checkQuery(sparkSession.sql(
      s"select * from table where ${colName} ${operator} cast(1 as ${t})"), expectedSkippedFiles)

    // checking without cast
    if (t != "short" && t != "byte") {
      logInfo(s"Checking skipping on ${colName} without using cast")
      checkQuery(sparkSession.sql(
        s"select * from table where ${colName} ${operator} 1"), expectedSkippedFiles)
    }
  }

  def setParquetMinMaxOptimization(enabled: Boolean): Unit = {
    val params = Map[String, String](
      (XskipperConf.XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED_KEY -> enabled.toString))
    Xskipper.setConf(params)
  }
}
