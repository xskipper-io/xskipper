/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import com.ibm.metaindex.metadata.index.types.BloomFilterMetaData
import io.xskipper.configuration.XskipperConf
import io.xskipper.index.Index
import io.xskipper.metadatastore.MetadataVersionStatus.MetadataVersionStatus
import io.xskipper.metadatastore.{MetadataHandle, MetadataStoreManager, MetadataStoreManagerType}
import io.xskipper.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileIndex}
import org.apache.spark.sql.types.ParquetMetadataStoreUDTRegistration

// define the [[MetadataStoreManagerType]]
case object Parquet extends MetadataStoreManagerType

/**
  * A MetadataBackend backed by Parquet objects
  */
object ParquetMetadataStoreManager extends MetadataStoreManager with Logging {
  /**
    * @return the [[MetadataStoreManagerType]] associated with this backend
    */
  override def getType: MetadataStoreManagerType = Parquet

  /**
    * Creates a new instance [[MetadataHandle]]
    * Used by [[getOrCreateMetadataHandle]] in order to create new instances
    * @param sparkSession
    * @param tableIdentifier
    * @return
    */
  override def createMetadataHandle(sparkSession: SparkSession,
                                    tableIdentifier: String): MetadataHandle = {
    new ParquetMetadataHandle(sparkSession, tableIdentifier)
  }

  /**
    * Returns a map of parameters to be set by the [[io.xskipper.search.DataSkippingFileFilter]]
    * on the [[MetadataHandle]] when using this [[MetadataStoreManager]].
    * This map can be used to pass specific parameters depending on the type of FileIndex that is
    * being replaced.
    * For example - in [[ParquetMetadataStoreManager]] we can
    * specify that the metadata location should be inferred from the table/default db properties
    *
    * @param tid          the table identifier for which the DataSkippingFileFilter will be created
    * @param sparkSession the sparks session
    * @param fileIndex    the fileIndex that will be replaced by
    *                     [[io.xskipper.search.DataSkippingFileFilter]]
    * @return a map of parameters to be set on the MetadataStore when the DataSkippingFileFilter
    *         is created
    */
  override def getDataSkippingFileFilterParams(tid: String,
                                sparkSession: SparkSession,
                                fileIndex: FileIndex): Map[String, String] = {
    var params = Map.empty[String, String]
    fileIndex match {
      case catalogFileIndex: CatalogFileIndex =>
        catalogFileIndex.table.identifier.database match {
          case Some(db) =>
            params += (ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY ->
              s"${db}.${catalogFileIndex.table.identifier.table}")
          case _ =>
            params += (ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY ->
              // scalastyle:off line.size.limit
              s"${sparkSession.sessionState.catalog.getCurrentDatabase}.${catalogFileIndex.table.identifier.table}")
          // scalastyle:on line.size.limit
        }
        params += (ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY ->
          ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME)
      case _ =>
    }
    params
  }

  /**
    * A unit function for setting up the [[MetadataStoreManager]].
    * The setup is called once when the [[MetadataStoreManager]] is registered
    */
  override def init(): Unit = {
    // Register all needed UDTs
    ParquetMetadataStoreUDTRegistration.registerAllUDTs()
    // Legacy bloom filter support
    ParquetMetadataStoreUDTRegistration.registerUDT(classOf[BloomFilterMetaData].getName,
      // scalastyle:off line.size.limit
      "com.ibm.metaindex.metadata.metadatastore.parquet.ParquetBaseClauseTranslator$BloomFilterMetaDataTypeUDT")
      // scalastyle:on line.size.limit
  }

  /**
    * @return Map of indexed datasets to their respective md version status and indexes
    */
  override def listIndexedDatasets(sparkSession: SparkSession):
  Map[String, (MetadataVersionStatus, Seq[Index])] = {
    // List indexed datasets at the configured base md path
    val metadataLocations = getIndexedDatasetsMetadataLocations(sparkSession)
    // Load datasets metadata
    val res: Seq[(String, (MetadataVersionStatus, Seq[Index]))] =
      metadataLocations.flatMap(indexMDFile =>
        try {
          val schema = ParquetUtils.mdFileToDF(sparkSession, indexMDFile).schema
          ParquetUtils.getTableIdentifier(schema) match {
            case Some(tid) =>
              val mdHandle = getOrCreateMetadataHandle(sparkSession, tid)
              Some(tid, (mdHandle.getMdVersionStatus(), mdHandle.getIndexes()))
            case None =>
              logWarning(s"Metadata file ${indexMDFile} is corrupted. tableIdentifier not found")
              None // Not a metadata obj, skip
          }
        }
        catch {
          case _: ParquetMetaDataStoreException =>
            logDebug(s"${indexMDFile} is not a valid dataset metadata file")
            None
        })
    res.toMap
  }

  private def getIndexedDatasetsMetadataLocations(sparkSession: SparkSession): Seq[String] = {
    val mdLocation: String = XskipperConf.getConf(ParquetMetadataStoreConf.PARQUET_MD_LOCATION)
    val mdType: String = XskipperConf.getConf(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE)

    val basePath = ParquetMetadataPath.getBaseMetadataPath(sparkSession, mdLocation, mdType)
    val conf = basePath.toUri.getScheme match {
      case "cos" =>
        // IBM COS may use flat listing so force non flat listing without effecting others
        val newConf = new Configuration(sparkSession.sparkContext.hadoopConfiguration)
        newConf.set("fs.cos.flat.list", "false")
        newConf.set("fs.cos.impl.disable.cache", "true")
        newConf
      case _ => sparkSession.sparkContext.hadoopConfiguration
    }
    val fs = basePath.getFileSystem(conf)
    if (fs.exists(basePath)) {
      fs.listStatus(basePath).filter(_.isDirectory).map(p => p.getPath.toString)
    }
    else {
      Seq.empty[String]
    }
  }

  /**
    * @return a map representing relevant [[MetadataStoreManager]] parameters
    */
  override def getParams(sparkSession: SparkSession): Map[String, String] = {
    val mdLocation = XskipperConf.getConf(ParquetMetadataStoreConf.PARQUET_MD_LOCATION)
    val mdType = XskipperConf.getConf(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE)
    val path = Utils.getPathDisplayName(
      ParquetMetadataPath.getBaseMetadataPath(sparkSession, mdLocation, mdType).toString)
    Map("Metadata base path" -> path)
  }
}
