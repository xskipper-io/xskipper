/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.configuration

import io.xskipper.utils.Utils
import io.xskipper.utils.identifier.Identifier
import org.apache.spark.internal.Logging

/**
  * JVM wide xskipper + metadatastore specific parameters
  * such as user, password, host.
  */
object XskipperConf extends Configuration with Logging {
  // override to enable custom action after setting for example:
  // the set function to reload identifier class
  override def set(key: String, value: String): Unit = {
    super.set(key, value)
    // reload identifier class if needed
    if (key == XSKIPPER_IDENTIFIER_CLASS_KEY) {
      // try loading the class dynamically
      Utils.getClassInstance[Identifier](value) match {
        case Some(identifier) => Utils.identifier = identifier
        case _ =>
      }
    }
  }

  // the prefix to all xskipper configurations
  val XSKIPPER_CONF_PREFIX: String = "io.xskipper."
  // legacy metaindex prefix
  private[xskipper] val METAINDEX_CONF_PREFIX = "spark.ibm.metaindex."

  // xskipper configurations
  val XSKIPPER_EVALUATION_ENABLED_KEY: String = XSKIPPER_CONF_PREFIX + "evaluation.enabled"
  val XSKIPPER_EVALUATION_ENABLED: ConfigEntry[Boolean] = ConfigEntry[Boolean](
    XSKIPPER_EVALUATION_ENABLED_KEY,
    defaultValue = false,
    doc =
      s"""When true, queries will run in evaluation mode.
         |when running in evaluation mode all of the indexed dataset will only
         |be processed for skipping stats and no data will be read
         |the evaluation mode is useful when we want to inspect the skipping stats
         |we might get for a given query
         |""".stripMargin
  )

  // defines the number of concurrent objects that will be indexed
  val XSKIPPER_INDEX_CREATION_PARALLELISM_KEY: String = XSKIPPER_CONF_PREFIX + "index.parallelism"
  val XSKIPPER_INDEX_CREATION_PARALLELISM: ConfigEntry[Int] = ConfigEntry[Int](
    XSKIPPER_INDEX_CREATION_PARALLELISM_KEY,
    defaultValue = 10,
    doc = "defines the number of concurrent objects that will be indexed",
    validationFunction = Some(((parallelism: Int) => parallelism > 0,
      "Index parallelism must be greater than 0"))
  )

  val XSKIPPER_INDEX_CREATION_MIN_CHUNK_SIZE_KEY: String
  = XSKIPPER_CONF_PREFIX + "index.minchunksize"
  val XSKIPPER_INDEX_CREATION_MIN_CHUNK_SIZE: ConfigEntry[Int] = ConfigEntry[Int](
    XSKIPPER_INDEX_CREATION_MIN_CHUNK_SIZE_KEY,
    defaultValue = 1,
    doc =
      s"""Defines the minimum chunk size to be used when indexing.
         |The chunk size will be multiplied by 2 till reaching the metadataStore upload chunk size
         |""".stripMargin,
    validationFunction = Some(((size: Int) => size > 0,
      "Minimum chunk size must be greater than 0"))
  )

  private val BLOOM_FILTER_FPP_SUFFIX = "index.bloom.fpp"
  private[xskipper] val LEGACY_BLOOM_FILTER_FPP_KEY =
    METAINDEX_CONF_PREFIX + BLOOM_FILTER_FPP_SUFFIX
  val BLOOM_FILTER_FPP_KEY: String = XSKIPPER_CONF_PREFIX + BLOOM_FILTER_FPP_SUFFIX
  val BLOOM_FILTER_FPP: ConfigEntry[Double] = ConfigEntry[Double](
    BLOOM_FILTER_FPP_KEY,
    defaultValue = 0.01,
    doc = "Indicates the bloom filter default fpp",
    validationFunction = Some(((fpp: Double) => fpp > 0 && fpp < 1,
      "Bloom filter fpp must be between 0 to 1"))
  )

  private val BLOOM_FILTER_NDV_SUFFIX = "index.bloom.ndv"
  private[xskipper] val LEGACY_BLOOM_FILTER_NDV_KEY =
    METAINDEX_CONF_PREFIX + BLOOM_FILTER_NDV_SUFFIX
  val BLOOM_FILTER_NDV_KEY: String = XSKIPPER_CONF_PREFIX + BLOOM_FILTER_NDV_SUFFIX
  val BLOOM_FILTER_NDV: ConfigEntry[Long] = ConfigEntry[Long](
    BLOOM_FILTER_NDV_KEY,
    defaultValue = 100000L,
    doc = "Indicates the bloom filter expected number of distinct values",
    validationFunction = Some(((ndv: Long) => ndv > 0,
      "Bloom filter ndv must be greater than 0"))
  )

  val XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED_KEY: String =
    XSKIPPER_CONF_PREFIX + "index.minmax.readoptimized.parquet"
  val XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED: ConfigEntry[Boolean] = ConfigEntry[Boolean](
    XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED_KEY,
    defaultValue = true,
    doc =
      s"""Indicates whether the collection of min/max stats
         |for Numeric columns when working with Parquet objects will be done in
         |an optimized way by reading the stats from the Parquet footer
         |""".stripMargin
  )

  val XSKIPPER_PARQUET_MINMAX_OPTIMIZED_PARALLELISM_KEY: String =
    XSKIPPER_CONF_PREFIX + "index.minmax.readoptimized.parquet.parallelism"
  val XSKIPPER_PARQUET_MINMAX_OPTIMIZED_PARALLELISM: ConfigEntry[Int]
  = ConfigEntry[Int](
    XSKIPPER_PARQUET_MINMAX_OPTIMIZED_PARALLELISM_KEY,
    defaultValue = 10000,
    doc =
      s"""The number of objects to be indexed in parallel when having only minmax indexes
         |on parquet objects
         |""".stripMargin,
    validationFunction = Some(((parallelism: Int) => parallelism > 0,
      "Optimized minmax indexing parallelism must be greater than 0"))
  )

  val XSKIPPER_MINMAX_IN_FILTER_THRESHOLD_KEY: String = XSKIPPER_CONF_PREFIX +
    "index.minmax.inFilterThreshold"
  val XSKIPPER_MINMAX_IN_FILTER_THRESHOLD: ConfigEntry[Int] = ConfigEntry[Int](
    XSKIPPER_MINMAX_IN_FILTER_THRESHOLD_KEY,
    defaultValue = 100,
    doc =
      s"""defines number of values in an IN filter above we will push down only
         |one min/max condition based on the min and maximum of the entire list
         |on parquet objects
         |""".stripMargin,
    validationFunction = Some(((threshold: Int) => threshold > 0,
      "minmax index IN threshold must be greater than 0"))
  )

  val XSKIPPER_TIMEOUT_KEY: String = XSKIPPER_CONF_PREFIX + "timeout"
  val XSKIPPER_TIMEOUT: ConfigEntry[Int] = ConfigEntry[Int](
    XSKIPPER_TIMEOUT_KEY,
    defaultValue = 10,
    doc =
      s"""The timeout in minutes to retrieve the indexed objects and the
         |relevant objects for the query from the metadatastore
         |""".stripMargin,
    validationFunction = Some(((size: Int) => size > 0,
      "Timeout must be greater than 0"))
  )

  val XSKIPPER_INDEX_DRIVER_MEMORY_FRACTION_KEY: String
  = XSKIPPER_CONF_PREFIX + "index.memoryFraction"
  val XSKIPPER_INDEX_DRIVER_MEMORY_FRACTION: ConfigEntry[Double]
  = ConfigEntry[Double](
    XSKIPPER_INDEX_DRIVER_MEMORY_FRACTION_KEY,
    defaultValue = 0.2,
    doc =
      s"""The memory fraction from the driver memory that indexing will use
         |in order to determine the maximum chunk size dynamically
         |""".stripMargin,
    validationFunction = Some(((fraction: Double) => fraction > 0 && fraction < 1,
      "xskipper memory fraction must be between 0 to 1"))
  )

  val XSKIPPER_IDENTIFIER_CLASS_KEY: String = XSKIPPER_CONF_PREFIX + "identifierclass"
  val XSKIPPER_IDENTIFIER_CLASS: ConfigEntry[String] = ConfigEntry[String](
    XSKIPPER_IDENTIFIER_CLASS_KEY,
    defaultValue = "io.xskipper.utils.identifier.Identifier",
    doc =
      s"""An identifier class to be loaded using reflection
         |used to specify how the table identifier and file ID are determined
         |""".stripMargin
  )
}
