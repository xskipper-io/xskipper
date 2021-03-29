/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.configuration

import io.xskipper.Xskipper
import io.xskipper.utils.Utils
import io.xskipper.utils.identifier.IBMCOSIdentifier
import org.scalatest.FunSuite

import scala.collection.mutable

class XskipperConfSuite extends FunSuite {
  test("Test configuration setting - valid values") {
    // first clear conf to make sure we start fresh
    XskipperConf.clearConf()
    // test valid values for all configuration parameters
    val params = mutable.HashMap[String, String]()

    params.put("io.xskipper.evaluation.enabled", "true")
    params.put("io.xskipper.stdoutprint.enabled", "true")
    params.put("io.xskipper.index.parallelism", "12")
    params.put("io.xskipper.index.minchunksize", "50")
    params.put("io.xskipper.index.bloom.fpp", "0.72")
    params.put("io.xskipper.index.bloom.ndv", "500")
    params.put("io.xskipper.index.minmax.readoptimized.parquet", "false")
    params.put("io.xskipper.index.minmax.readoptimized.parquet.parallelism",
      "500")
    params.put("io.xskipper.timeout", "9")
    params.put("io.xskipper.index.memoryFraction", "0.1")
    params.put("io.xskipper.identifierclass",
      "io.xskipper.utils.identifier.IBMCloudSQLQueryIdentifier")
    params.put("io.xskipper.index.minmax.inFilterThreshold", "50")

    Xskipper.setConf(params.toMap)

    assertResult(true)(XskipperConf.getConf(XskipperConf.XSKIPPER_EVALUATION_ENABLED))
    assertResult(12)(XskipperConf.getConf(XskipperConf.XSKIPPER_INDEX_CREATION_PARALLELISM))
    assertResult(50)(XskipperConf.getConf(XskipperConf.XSKIPPER_INDEX_CREATION_MIN_CHUNK_SIZE))
    assertResult(0.72)(XskipperConf.getConf(XskipperConf.BLOOM_FILTER_FPP))
    assertResult(500)(XskipperConf.getConf(XskipperConf.BLOOM_FILTER_NDV))
    assertResult(false)(XskipperConf.getConf(XskipperConf.XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED))
    assertResult(9)(XskipperConf.getConf(XskipperConf.XSKIPPER_TIMEOUT))
    assertResult(0.1)(XskipperConf.getConf(XskipperConf.XSKIPPER_INDEX_DRIVER_MEMORY_FRACTION))
    assertResult("io.xskipper.utils.identifier.IBMCloudSQLQueryIdentifier")(
      XskipperConf.getConf(XskipperConf.XSKIPPER_IDENTIFIER_CLASS))
    assertResult(50)(XskipperConf.getConf(XskipperConf.XSKIPPER_MINMAX_IN_FILTER_THRESHOLD))
  }

  test("Test configuration setting - invalid values - assert fallback to default") {
    // first clear conf to make sure we start fresh
    XskipperConf.clearConf()
    // test invalid values for all configuration parameters
    // to see that there is a fallback to the default configuration
    val params = mutable.HashMap[String, String]()

    params.put("io.xskipper.evaluation.enabled", "sda")
    params.put("io.xskipper.stdoutprint.enabled", "fre")
    params.put("io.xskipper.index.parallelism", "-1")
    params.put("io.xskipper.index.minchunksize", "-50")
    params.put("io.xskipper.index.bloom.fpp", "1.5")
    params.put("io.xskipper.index.bloom.ndv", "-100")
    params.put("io.xskipper.index.minmax.readoptimized.parquet", "fe")
    params.put("io.xskipper.index.minmax.readoptimized.parquet.parallelism",
      "-1")
    params.put("io.xskipper.timeout", "-2")
    params.put("io.xskipper.index.memoryFraction", "1.5")
    params.put("io.xskipper.index.minmax.inFilterThreshold", "-20")

    Xskipper.setConf(params.toMap)

    def assertDefault[T](entry: ConfigEntry[T]): Unit = {
      assertResult(entry.defaultValue)(XskipperConf.getConf(entry))
    }

    assertDefault(XskipperConf.XSKIPPER_EVALUATION_ENABLED)
    assertDefault(XskipperConf.XSKIPPER_INDEX_CREATION_PARALLELISM)
    assertDefault(XskipperConf.XSKIPPER_INDEX_CREATION_MIN_CHUNK_SIZE)
    assertDefault(XskipperConf.BLOOM_FILTER_FPP)
    assertDefault(XskipperConf.BLOOM_FILTER_NDV)
    assertDefault(XskipperConf.XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED)
    assertDefault(XskipperConf.XSKIPPER_TIMEOUT)
    assertDefault(XskipperConf.XSKIPPER_INDEX_DRIVER_MEMORY_FRACTION)
    assertDefault(XskipperConf.XSKIPPER_MINMAX_IN_FILTER_THRESHOLD)
  }

  test("setting custom identifier class using set") {
    Xskipper.set("io.xskipper.identifierclass",
      "io.xskipper.utils.identifier.IBMCOSIdentifier")
    assert(Utils.identifier.isInstanceOf[IBMCOSIdentifier])
  }

  test("setting custom identifier class using set conf") {
    Xskipper.setConf(Map("io.xskipper.identifierclass"->
      "io.xskipper.utils.identifier.IBMCOSIdentifier"))
    assert(Utils.identifier.isInstanceOf[IBMCOSIdentifier])
  }
}
