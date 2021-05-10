/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.configuration.ConfigEntry

object ParquetMetadataStoreConf {
  // the prefix to all Parquet metadatastore configurations
  val PARQUET_METADATASTORE_CONF_PREFIX = "io.xskipper.parquet."
  val LEGACY_PARQUET_METADATASTORE_CONF_PREFIX = "spark.ibm.metaindex.parquet."
  // Metadata storage version
  val PARQUET_MD_STORAGE_VERSION: Long = 4L
  // Minimum version supported
  val PARQUET_MINIMUM_SUPPORTED_MD_STORAGE_VERSION: Long = 1L

  // Parquet metadatastore specific configurations
  private[parquet] val PARQUET_MD_LOCATION_SUFFIX = "mdlocation"
  private[xskipper] val LEGACY_PARQUET_MD_LOCATION_KEY =
    LEGACY_PARQUET_METADATASTORE_CONF_PREFIX + PARQUET_MD_LOCATION_SUFFIX
  val PARQUET_MD_LOCATION_KEY = PARQUET_METADATASTORE_CONF_PREFIX + PARQUET_MD_LOCATION_SUFFIX
  val PARQUET_MD_LOCATION = ConfigEntry[String](
    PARQUET_MD_LOCATION_KEY,
    defaultValue = "",
    doc = "The metadata location according to the type\n"
  )

  // The type of URL that appears in the metadata location
  // The type can be:
  //   type = EXPLICIT_METADATA_LOCATION => an explicit location of the metadata
  //   type = EXPLICIT_BASE_PATH_LOCATION => an explicit definition of the base path
  //                                         of the metadata
  //   type = HIVE_TABLE_NAME => the name of the hive table that contains in the table properties
  //                             the exact path of the metadata
  //                             Note that if the parameter contains only the table name then the
  //                             current database is used
  //   type = HIVE_DB_NAME => the name of the hive database that contains in the db properties the
  //                          base path of the metadata
  val PARQUET_MD_LOCATION_EXPLICIT_LOCATION = "EXPLICIT_LOCATION"
  val PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION = "EXPLICIT_BASE_PATH_LOCATION"
  val PARQUET_MD_LOCATION_HIVE_TABLE_NAME = "HIVE_TABLE_NAME"
  val PARQUET_MD_LOCATION_HIVE_DB_NAME = "HIVE_DB_NAME"


  val PARQUET_MD_LOCATION_TYPE_KEY = PARQUET_METADATASTORE_CONF_PREFIX + "mdlocation.type"
  val PARQUET_MD_LOCATION_TYPE = ConfigEntry[String](
    PARQUET_MD_LOCATION_TYPE_KEY,
    defaultValue = PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION,
    doc =
      s"""The type of the metadata location
         |The type can be:
         |type = EXPLICIT_METADATA_LOCATION => an explicit location of the metadata
         |type = EXPLICIT_BASE_PATH_LOCATION => an explicit definition of the base path of the
         |                                      metadata
         |type = HIVE_TABLE_NAME => the name of the hive table that contains in the table
         |                          properties the exact path of the metadata
         |                          Note that if the parameter contains only the table name then
         |                          the current database is used
         |type = HIVE_DB_NAME => the name of the hive database that contains in the db properties
         |                       the base path of the metadata
         |
         |""".stripMargin,
    validationFunction = Some(((locationType: String) => locationType match {
      case PARQUET_MD_LOCATION_EXPLICIT_LOCATION => true
      case PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION => true
      case PARQUET_MD_LOCATION_HIVE_TABLE_NAME => true
      case PARQUET_MD_LOCATION_HIVE_DB_NAME => true
      case _ => false
    }, "metadata location type must be one of the following:\n" +
      s"${PARQUET_MD_LOCATION_EXPLICIT_LOCATION} - an explicit location of the metadata\n" +
      s"${PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION} -" +
      s" an explicit definition of the base path of the metadata\n" +
      s"${PARQUET_MD_LOCATION_HIVE_TABLE_NAME} -" +
      s"the name of the hive table that contains in the table properties" +
      s" the exact path of the metadata\n" +
      s"${PARQUET_MD_LOCATION_HIVE_DB_NAME} -" +
      s" the name of the hive database that contains in the" +
      s" db properties the base path of the metadata"))
  )

  val PARQUET_INDEX_CHUNK_SIZE_KEY = PARQUET_METADATASTORE_CONF_PREFIX + "index.chunksize"
  val PARQUET_INDEX_CHUNK_SIZE = ConfigEntry[Int](
    PARQUET_INDEX_CHUNK_SIZE_KEY,
    defaultValue = 25000,
    doc = "The number of objects to index in each chunk",
    validationFunction = Some(((size: Int) => size > 0,
      "Chunk size must be greater than 0"))
  )

  val PARQUET_MAX_RECORDS_PER_METADATA_FILE_KEY =
    PARQUET_METADATASTORE_CONF_PREFIX + "maxRecordsPerMetadataFile"
  val PARQUET_MAX_RECORDS_PER_METADATA_FILE = ConfigEntry[Int](
    PARQUET_MAX_RECORDS_PER_METADATA_FILE_KEY,
    defaultValue = 50000,
    doc =
      s"""The number of records per metadata file used in the compact stage to limit the
         | number of records for small sized metadata files.
         |""".stripMargin,
    validationFunction = Some(((numRecords: Int) => numRecords > 0,
      "Max records per file must be greater than 0"))
  )

  val PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE_KEY =
    PARQUET_METADATASTORE_CONF_PREFIX + "maxMetadataFileSize"
  val PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE =
    ConfigEntry[Long](
      PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE_KEY,
      defaultValue = 33554432L, // 32MB
      doc =
        s"""The expected max size of each metadata file will be used by the compact
           | function to distribute the data to multiple files to distribute the data to multiple
           | files in such way that each is less than the expected max size of each metadata file
           | used in conjunction with the max records per file configuration
           |""".stripMargin,
      validationFunction = Some(((size: Long) => size > 0,
        "max metadata file size must be greater than 0"))
    )

  val DEDUP_ON_REFRESH_KEY =
    PARQUET_METADATASTORE_CONF_PREFIX + "refresh.dedup"
  val DEDUP_ON_REFRESH =
    ConfigEntry[Boolean](
      DEDUP_ON_REFRESH_KEY,
    defaultValue = true,
    doc =
      s"""Whether to run distinct when running refresh
         |This is used to cleanup failures in refresh which might introduced duplicate rows
         |""".stripMargin
  )

  val DEDUP_ON_FILTER_KEY =
    PARQUET_METADATASTORE_CONF_PREFIX + "filter.dedup"
  val DEDUP_ON_FILTER =
    ConfigEntry[Boolean](
      DEDUP_ON_FILTER_KEY,
      defaultValue = true,
      doc =
        s"""Whether to run distinct during filtering
           |This is used to cleanup failures in refresh which might introduced duplicate rows
           |""".stripMargin
    )

  // configuration keys for Encryption
  val PARQUET_FOOTER_KEY_SPARK_KEY = "encryption.footer.key"
  val PARQUET_FOOTER_KEY_PARAM_KEY =
    PARQUET_METADATASTORE_CONF_PREFIX + PARQUET_FOOTER_KEY_SPARK_KEY

  val PARQUET_PLAINTEXT_FOOTER_SPARK_KEY = "encryption.plaintext.footer"
  val PARQUET_PLAINTEXT_FOOTER_ENABLED_KEY =
    PARQUET_METADATASTORE_CONF_PREFIX + PARQUET_PLAINTEXT_FOOTER_SPARK_KEY
  val PARQUET_PLAINTEXT_FOOTER_ENABLED = ConfigEntry[Boolean](
    PARQUET_PLAINTEXT_FOOTER_ENABLED_KEY,
    defaultValue = false,
    doc = "Whether or not to use plain footer"
  )

  val PARQUET_COLUMN_KEYS_SPARK_KEY = "encryption.column.keys"
}
