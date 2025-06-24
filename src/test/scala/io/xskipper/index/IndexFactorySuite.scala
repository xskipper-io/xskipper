/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package io.xskipper.index

import io.xskipper.configuration.XskipperConf
import org.scalatest.funsuite.AnyFunSuite

class IndexFactorySuite extends AnyFunSuite {

  test("Verify Bloom Filter is correctly reconstructed from legacy parameters") {
    val fpp = 0.123
    val ndv = 1234L
    val col = "foo"
    val keyMetadata = None
    // check when both fpp and ndv are passes
    val allParams = Map[String, String](
      XskipperConf.LEGACY_BLOOM_FILTER_FPP_KEY -> fpp.toString,
      XskipperConf.LEGACY_BLOOM_FILTER_NDV_KEY -> ndv.toString
    )
    val expectedAllParams = BloomFilterIndex(col, fpp, ndv, keyMetadata)
    val actualAllParams = BaseIndexFactory.getIndex("bloomfilter",
      Seq(col), allParams, keyMetadata)
    assertResult(Some(expectedAllParams))(actualAllParams)

    // check when only fpp is passed
    val onlyFppParams = Map[String, String](
      XskipperConf.LEGACY_BLOOM_FILTER_FPP_KEY -> fpp.toString
    )
    val expectedOnlyFpp = BloomFilterIndex(col, fpp, keyMetadata = keyMetadata)
    val actualOnlyFpp = BaseIndexFactory.getIndex("bloomfilter",
      Seq(col), onlyFppParams, keyMetadata)
    assertResult(Some(expectedOnlyFpp))(actualOnlyFpp)

    // check when No parameter is passed
    val expectedNoParams = BloomFilterIndex(col, keyMetadata = keyMetadata)
    val actualNoParams = BaseIndexFactory.getIndex("bloomfilter",
      Seq(col), Map.empty, keyMetadata)
    assertResult(Some(expectedNoParams))(actualNoParams)
  }


  test("Verify Bloom Filter is correctly reconstructed from current parameters") {
    val fpp = 0.123
    val ndv = 1234L
    val col = "foo"
    val keyMetadata = None
    // check when both fpp and ndv are passes
    val allParams = Map[String, String](
      XskipperConf.BLOOM_FILTER_FPP_KEY -> fpp.toString,
      XskipperConf.BLOOM_FILTER_NDV_KEY -> ndv.toString
    )
    val expectedAllParams = BloomFilterIndex(col, fpp, ndv, keyMetadata)
    val actualAllParams = BaseIndexFactory.getIndex("bloomfilter",
      Seq(col), allParams, keyMetadata)
    assertResult(Some(expectedAllParams))(actualAllParams)

    // check when only fpp is passed
    val onlyFppParams = Map[String, String](
      XskipperConf.BLOOM_FILTER_FPP_KEY -> fpp.toString
    )
    val expectedOnlyFpp = BloomFilterIndex(col, fpp, keyMetadata = keyMetadata)
    val actualOnlyFpp = BaseIndexFactory.getIndex("bloomfilter",
      Seq(col), onlyFppParams, keyMetadata)
    assertResult(Some(expectedOnlyFpp))(actualOnlyFpp)

    // check when No parameter is passed
    val expectedNoParams = BloomFilterIndex(col, keyMetadata = keyMetadata)
    val actualNoParams = BaseIndexFactory.getIndex("bloomfilter",
      Seq(col), Map.empty, keyMetadata)
    assertResult(Some(expectedNoParams))(actualNoParams)
  }
}
