/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.status.Status
import io.xskipper.utils.Utils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

object ParquetMetadataPath extends Logging {
  /**
    * Constructs the metadata path for a given base path and a uri by concatenating
    * sha256 digest of the uri to the base path
    *
    * @param basePath the base path
    * @param uri      the uri of the dataset (can be either a path or table identifier)
    * @return the path of the metadata
    */
  def getMetadataPath(basePath: String, uri: String): Path = {
    // try to convert the base path to a valid path
    val path: Path = convertMetaDataLocationToPath(basePath)
    // concatenate sha256 crypto 256 bit hash (64 characters) - collision chances are negligible
    new Path(path, DigestUtils.sha256Hex(uri))
  }

  /**
    * Wrapper for stringToPath to return backend specific error message
    *
    * @param mdLocation string to convert to org.apache.hadoop.fs.Path
    * @return metadata hadoop fs path
    * @throws ParquetMetaDataStoreException if invalid path
    */
  def convertMetaDataLocationToPath(mdLocation: String): Path = {
    try {
      Utils.stringToPath(mdLocation)
    } catch {
      case e: Exception =>
        throw ParquetMetaDataStoreException(
          s"""ParquetMetaDataStore initialization error:
           Invalid path, error resolving from the given metadata location path""", e)
    }
  }

  /**
    * Given a db name extracts the base path from the database parameters
    */
  def getBasePathFromDB(sparkSession: SparkSession,
                        db: String): String = {
    val dbProperties = sparkSession.sessionState.catalog.getDatabaseMetadata(db).properties
    dbProperties.get(
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY) match {
      case Some(basePath) => basePath
      case _ if dbProperties.contains(ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY) =>
        dbProperties(ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY)
      case _ =>
        throw ParquetMetaDataStoreException("Unable to resolve metadata location - " +
          s"DB properties does not contain the metadata base location")
    }
  }

  /**
    * Resolves the metadata path according to the given parameters
    *
    * @param sparkSession     [[SparkSession]] instance for accessing the catalog if needed
    * @param uri              the URI of the dataset
    * @param metaDataLocation depending on the type can be:
    *                         type = EXPLICIT_METADATA_LOCATION => an explicit location of
    *                         the metadata
    *                         type = EXPLICIT_BASE_PATH_LOCATION => an explicit definition of the
    *                         base path of the metadata
    *                         type = HIVE_TABLE_NAME => the name of the hive table that contains
    *                         the table properties
    *                         the exact path of the metadata.
    *                         Note that if the parameter contains only the table name then the
    *                         current database is used
    *                         type = HIVE_DB_NAME => the name of the hive database that
    *                         contains in the db properties the
    *                         base path of the metadata
    * @return the path of the metadata
    */
  def resolveMetadataPath(sparkSession: SparkSession, uri: String,
                          metaDataLocation: String, `type`: String): Path = {
    `type` match {
      // metaDataLocation is explicit metadata location
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_LOCATION =>
        ParquetMetadataPath.convertMetaDataLocationToPath(metaDataLocation)
      // metaDataLocation is a base path location
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION =>
        ParquetMetadataPath.getMetadataPath(metaDataLocation, uri)
      // metaDataLocation is in the table definition
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME =>
        // check if the identifier contains a table name
        val table: TableIdentifier = metaDataLocation.contains(".") match {
          case true =>
            // extract the db name
            TableIdentifier(metaDataLocation.substring(metaDataLocation.indexOf(".") + 1),
              Some(metaDataLocation.substring(0, metaDataLocation.indexOf("."))))
          case _ =>
            // use the current database
            logInfo(s"Table identifier does not contain database - " +
              s"using current database")
            TableIdentifier(metaDataLocation.substring(metaDataLocation.indexOf(".")))
        }
        val tableProperties = sparkSession.sessionState.catalog.getTableMetadata(table).properties
        tableProperties.get(
          ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY) match {
          case Some(path) => ParquetMetadataPath.convertMetaDataLocationToPath(path)
          case _ if tableProperties.contains(
            ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY) =>
            val path = tableProperties(ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY)
            ParquetMetadataPath.convertMetaDataLocationToPath(path)
          case _ =>
            logInfo(s"Table doesn't contain the metadata location parameter - " +
              "falling back to database property")
            // using the table database if present otherwise using the current database
            val tableDatabase = table.database match {
              case Some(db) => db
              case _ => sparkSession.sessionState.catalog.getCurrentDatabase
            }
            resolveMetadataPath(sparkSession, uri, tableDatabase,
              ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_DB_NAME)
        }
      // metaDataLocation is in the database parameters
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_DB_NAME =>
        // extract the metadata base path from the db properties
        val basePath = getBasePathFromDB(sparkSession, metaDataLocation)
        getMetadataPath(basePath, uri)
      case _ =>
        throw ParquetMetaDataStoreException("Invalid metadata location type specified")
    }
  }

  /**
    *
    * @return Metadata base location
    * @throws ParquetMetaDataStoreException if invalid metaDataLocation or invalid metadataType
    */
  def getBaseMetadataPath(sparkSession: SparkSession,
                          metaDataLocation: String, metadataType: String): Path = {
    var location = metaDataLocation
    metadataType match {
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_LOCATION |
           ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME =>
        throw ParquetMetaDataStoreException(
          s"""Parquet metadatastore configuration not valid.
          Valid values are: ${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_DB_NAME},
          ${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION}""")
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_DB_NAME =>
        location = ParquetMetadataPath.getBasePathFromDB(sparkSession, location)
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_EXPLICIT_BASE_PATH_LOCATION =>
      case _ =>
        throw ParquetMetaDataStoreException(Status.invalidConfig("metadata location type"))
    }
    ParquetMetadataPath.convertMetaDataLocationToPath(location)
  }
}

case class ParquetMetadataPath(path: Path, `type`: String) extends {
  def this(sparkSession: SparkSession, uri: String,
           metaDataLocation: String, `type`: String) = {
    this(ParquetMetadataPath.resolveMetadataPath(sparkSession, uri, metaDataLocation, `type`),
      `type`)
  }
}
