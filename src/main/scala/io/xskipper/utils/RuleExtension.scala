/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.datasources.xskipper.DataSkippingFileIndexRule

/**
  * Injects the DataSkippingFileIndexRule into catalyst as part of
  * the operatorOptimization rules and enable it
  * using spark session extensions injectOptimizerRule.
  *
  * To use with regular spark session (either pyspark or scala):
  * val spark = SparkSession
  * .builder()
  * .appName("Xskipper")
  * .config("spark.master", "local[*]") // comment out to run in production
  * .config("spark.sql.extensions", "io.xskipper.utils.RuleExtension")
  * //.enableHiveSupport()
  * .getOrCreate()
  *
  * To use with thrift server:
  * 1) get a Xskipper jar file (or specify packages parameter)
  * 2) start the thrift server, with the extension:
  *   start-thriftserver.sh --jars <XskipperJar>
  *                         --conf spark.sql.extensions=io.xskipper.utils.RuleExtension
  *    Alternatively, instead of --jars use --packages io.xskipper:xskipper-coreX:Y
  * 3) you can now connect via JDBC (e.g. - beeline/squirrel/ any other JDBC driver)
  */

class RuleExtension extends (SparkSessionExtensions => Unit) with Logging{
  def apply(ex : SparkSessionExtensions): Unit = {
    logInfo("Injecting RuleBuilder for DataSkippingFileIndexRule and enabling Data Skipping")
    // enable the data skipping rule
    // Note: this needed since most use cases for injecting the rule with the spark.sql.extensions
    // config parameter don't have the ability to run custom code
    // such as the thrift server
    val rule = new DataSkippingFileIndexRule
    rule.enableDataSkipping
    ex.injectOptimizerRule(_ => rule)
  }
}
