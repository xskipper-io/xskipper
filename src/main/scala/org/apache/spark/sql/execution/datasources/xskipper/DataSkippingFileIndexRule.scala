/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.execution.datasources.xskipper

import io.xskipper.Registration
import io.xskipper.configuration.XskipperConf
import io.xskipper.utils.Utils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.csv.CSVTable
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable
import org.apache.spark.sql.execution.datasources.v2.orc.OrcTable
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.datasources.v2.xskipper.{CSVDataSkippingTable, JsonDataSkippingTable, OrcDataSkippingTable, ParquetDataSkippingTable}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
  *
  * A Catalyst rule which replaces a logical relation plan's [[InMemoryFileIndex]]
  * with an extended [[DataSkippingFileIndex]] which allows fine grained file filtering.
  *
  * The following applies to the case where plan is an instance of LogicalRelation,
  * with a [[HadoopFsRelation]]:
  * the rule adds a dummy option to the options field of the new HadoopFsRelation.
  * if the input plan's catalog is not an instance of IndexedCatalog - then the
  * dummy option will be absent, thus it's addition will ensure that
  * [[org.apache.spark.sql.catalyst.trees.TreeNode.fastEquals]]
  * will return false when comparing the input plan to the returned plan.
  *
  * if the input plan's catalog is indeed an instance of IndexedCatalog -
  * then the plan doesn't change, and indeed
  * [[org.apache.spark.sql.catalyst.trees.TreeNode.fastEquals]] will return true -
  * preventing an unnecessary catalyst churn.
  *
  */
class DataSkippingFileIndexRule extends Rule[LogicalPlan] {
  // rule activation toggle
  private var ruleEnabled : Boolean = false

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // catch V2 relations with invalid schema - we don't want to touch them
    case r@DataSourceV2Relation(table: FileTable, _, _, _, _) if ruleEnabled
      && !Utils.isSchemaValid(r.schema) =>
      logWarning(s"Schema is invalid for " +
        s"${table.fileIndex.rootPaths.applyOrElse(0, "PATH_UNKNOWN")}," +
        s" no skipping will be attempted")
      r
    // catch V1 relations with invalid schema - we don't want to touch them
    case r@LogicalRelation(hfs: HadoopFsRelation, _, _, _) if ruleEnabled
      && !Utils.isSchemaValid(r.schema) =>
      logWarning(s"Schema is invalid for " +
        s"${hfs.location.rootPaths.applyOrElse(0, "PATH_UNKNOWN")}," +
        s" no skipping will be attempted")
      r
    /**
      * DataSource V2 support
      * Avoiding activation of the rule if:
      * 1. Current Table implementation is already a DataSkippingTable
      * (this is needed since both operator optimization batch is running till fixed point and this
      * rule has no need to run more than one time)
      * 2. The table schema is invalid
      */
    case r@DataSourceV2Relation(table: FileTable, _, _, _, _) if
          ruleEnabled &&
            !table.isInstanceOf[ParquetDataSkippingTable] &&
            !table.isInstanceOf[CSVDataSkippingTable] &&
            !table.isInstanceOf[OrcDataSkippingTable] &&
            !table.isInstanceOf[JsonDataSkippingTable] => {
      // In case of error the query will continue regularly without skipping
      try {
        // replacing the table's file index
        val newTable: Option[FileTable] = table match {
          case ParquetTable(name, sparkSession, options, paths,
          userSpecifiedSchema, fallbackFileFormat) =>
            Some(new ParquetDataSkippingTable(name, sparkSession, options, paths,
              userSpecifiedSchema, fallbackFileFormat))
          case CSVTable(name, sparkSession, options, paths,
          userSpecifiedSchema, fallbackFileFormat) =>
            Some(new CSVDataSkippingTable(name, sparkSession, options,
              paths, userSpecifiedSchema, fallbackFileFormat))
          case JsonTable(name, sparkSession, options, paths,
          userSpecifiedSchema, fallbackFileFormat) =>
            Some(new JsonDataSkippingTable(name, sparkSession, options,
              paths, userSpecifiedSchema, fallbackFileFormat))
          case OrcTable(name, sparkSession, options, paths,
          userSpecifiedSchema, fallbackFileFormat) =>
            Some(new OrcDataSkippingTable(name, sparkSession, options,
              paths, userSpecifiedSchema, fallbackFileFormat))
          case _ =>
            logInfo(s"Unknown file table ${table.getClass.toString} => No Skipping")
            None
        }

        // Replace table if needed
        newTable match {
          case Some(tbl) =>
            logInfo(s"Replacing ${r.table.getClass().toString} with ${tbl.getClass.toString}")
            r.copy(table = tbl,
              options = new CaseInsensitiveStringMap((r.options.asScala
                + ("DummyOption" -> "Dummy")).asJava))
          case _ => r
        }
      } catch {
        case e: Throwable =>
          logWarning(s"Data skipping rule failed, leaving relation unchanged", e)
          r
      }
    }
    /**
      * DataSource V1 support
      * Avoiding activation of the rule if:
      * 1. Current relation is a text relation - it turns out that for some formats such
      * as CSV and JSON the query plan for reading into the relation that is generated by calls
      * such as spark.read.csv(...) or spark.read.json(...) begins in a state in which the file
      * relations in the leafs are text relations, and the catalyst rules are applied on that plan
      * as well.
      * Among other things, these "intermediate" relations have an InMemoryFileIndex
      * (as their location) where ALL the objects that returned from the listing appear
      * as the rootPaths.
      * applying ther rule on these relations is unnecessary.
      * 2. current implementation is already a DataSkippingFileIndex
      * (this is needed since both operator optimization batch is running till fixed point and this
      * rule has no need to run more than one time)
      * 3. This is a streaming relation
      * 4. The schema is invalid
      */
    case l@LogicalRelation(hfs: HadoopFsRelation, _, _, isStreaming) if
            ruleEnabled && isStreaming == false &&
            !hfs.fileFormat.isInstanceOf[TextFileFormat] &&
            !hfs.location.isInstanceOf[InMemoryDataSkippingIndex] &&
            !hfs.location.isInstanceOf[CatalogDataSkippingFileIndex] =>
      // In case of error the query will continue regularly without skipping
      try {
        val spark = hfs.sparkSession
        val newOptions = hfs.options + ("DummyOption" -> "Dummy")
        hfs.location match {
          case inMemoryFileIndex : InMemoryFileIndex =>
            logInfo(s"Replacing logical relation ${hfs.toString} with file index" +
              s" ${hfs.location.toString} with File Skipping File Index..")
            // reconstructing FileStatusCache to avoid re listing
            val fileStatusCache = DataSkippingUtils.recreateFileStatusCache(spark,
              inMemoryFileIndex)
            // replace with dataskipping FileIndex
            val tableIdentifiers =
              hfs.location.rootPaths.map(p => Utils.getTableIdentifier(p.toUri.toString)).distinct
            // create the file filter according to the backend
            val metadataStoreManager = Registration.getActiveMetadataStoreManager()
            val ff = tableIdentifiers.map(tid =>
              DataSkippingUtils.getFileFilter(
                inMemoryFileIndex,
                tid,
                metadataStoreManager,
                spark,
                XskipperConf.getConf(XskipperConf.XSKIPPER_EVALUATION_ENABLED)))
            val dataSkippingFileIndex = new InMemoryDataSkippingIndex(
              spark,
              hfs.location.rootPaths,
              hfs.options,
              Option(inMemoryFileIndex.partitionSchema),
              fileStatusCache,
              Some(inMemoryFileIndex.partitionSpec()),
              inMemoryFileIndex.metadataOpsTimeNs,
              tableIdentifiers,
              ff,
              Registration.getCurrentMetadataFilterFactories(),
              Registration.getCurrentClauseTranslators(),
              Registration.getActiveMetadataStoreManagerType())
            // adding a new dummy option, to avoid issues around fastEquals.
            val newHfsRelation =
              hfs.copy(location = dataSkippingFileIndex, options = newOptions)(spark)
            l.copy(relation = newHfsRelation)
          // Note: the reason to keep the CatalogFileFilter if possible is to avoid unnecessary
          // listing and to "enjoy" the optimization that might come with it
          case catalogFileIndex: CatalogFileIndex =>
            logInfo(s"Replacing logical relation ${hfs.toString} with file index " +
              s"${hfs.location.toString} with File Skipping File Index..")
            // the table identifier in case of hive is the table identifier (<db.table>)
            val tableIdentifier =
              s"${catalogFileIndex.table.database}.${catalogFileIndex.table.identifier.table}"
            // create the file filter according to the backend
            val metadataStoreManager = Registration.getActiveMetadataStoreManager()
            val ff = DataSkippingUtils.getFileFilter(
              catalogFileIndex,
              tableIdentifier,
              metadataStoreManager,
              spark,
              XskipperConf.getConf(
                XskipperConf.XSKIPPER_EVALUATION_ENABLED))
            val newCatalog = new CatalogDataSkippingFileIndex(
              spark,
              catalogFileIndex.table,
              tableIdentifier,
              ff,
              catalogFileIndex.sizeInBytes,
              Registration.getCurrentMetadataFilterFactories(),
              Registration.getCurrentClauseTranslators(),
              Registration.getActiveMetadataStoreManagerType())
            // adding a new dummy option, to avoid issues around fastEquals.
            val newHfsRelation = hfs.copy(location = newCatalog, options = newOptions)(spark)
            l.copy(relation = newHfsRelation)
          // Unknown FileIndex implementation so leave the relation as it is
          case other =>
            logWarning(s"Data skipping rule encountered unknown FileIndex implementation -  " +
              s"${other.toString}, leaving relation unchanged")
            l
        }
      } catch {
        case e: Throwable =>
          logWarning(s"Data skipping rule failed, leaving relation unchanged", e)
          l
      }
  }

  /**
    * Enable data skipping rule
    */
  def enableDataSkipping: Unit = ruleEnabled = true

  /**
    * Returns the true if data skipping is enabled
    */
  def isEnabled: Boolean = ruleEnabled

  /**
    * Disable data skipping rule
    */
  def disableDataSkipping: Unit = ruleEnabled = false
}
