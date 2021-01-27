/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

import io.xskipper.index.{BaseIndexFactory, IndexFactory}
import io.xskipper.metadatastore.parquet.{Parquet, ParquetBaseClauseTranslator, ParquetBaseMetaDataTranslator, ParquetMetadataStoreManager}
import io.xskipper.metadatastore.{ClauseTranslator, MetaDataTranslator, MetadataStoreManager, MetadataStoreManagerType}
import io.xskipper.search.filters.{BaseFilterFactory, MetadataFilterFactory}
import io.xskipper.utils.Utils
import org.apache.spark.internal.Logging

import scala.collection.{immutable, mutable, _}

/**
  * Contains JVM wide default Factories and Translators for xskipper plugins
  */
object Registration extends Serializable with Logging {
  // Defaults - Parquet metadatastore
  private val indexFactoriesDefaults: immutable.Seq[IndexFactory] =
    immutable.Seq(BaseIndexFactory)
  private val metadataFilterFactoriesDefaults: immutable.Seq[MetadataFilterFactory] =
    immutable.Seq(BaseFilterFactory)
  private val clauseTranslatorsDefaults: immutable.Seq[ClauseTranslator] =
    immutable.Seq(ParquetBaseClauseTranslator)
  private val metaDataTranslatorsDefaults: immutable.Seq[MetaDataTranslator] =
    immutable.Seq(ParquetBaseMetaDataTranslator)
  // map of [[MetadataStoreManager]] supported
  private val metadataStoreManagersDefaults:
    mutable.Map[MetadataStoreManagerType, MetadataStoreManager] =
      mutable.Map((Parquet, ParquetMetadataStoreManager))
  private val metadataStoreManagerTypeDefault: MetadataStoreManagerType = Parquet

  // Current Factories and Translators
  private val indexFactories: mutable.Buffer[IndexFactory] = mutable.Buffer.empty
  private val metadataFilterFactories: mutable.Buffer[MetadataFilterFactory] = mutable.Buffer.empty
  private val clauseTranslators: mutable.Buffer[ClauseTranslator] = mutable.Buffer.empty
  private val metaDataTranslators: mutable.Buffer[MetaDataTranslator] = mutable.Buffer.empty
  private val metaDataStoreManagers: mutable.Map[MetadataStoreManagerType, MetadataStoreManager] =
    metadataStoreManagersDefaults

  // Setup default metadataStore
  private var activeMetadataStoreManagerType: MetadataStoreManagerType =
    metadataStoreManagerTypeDefault
  metaDataStoreManagers(metadataStoreManagerTypeDefault).init()

  setDefaultRegistrations()

  /**
    * get the currently registered [[IndexFactory]]-'s
    *
    * @return the currently registered [[IndexFactory]]-'s
    */
  def getCurrentIndexFactories(): scala.collection.immutable.Seq[IndexFactory] = {
    immutable.Seq(indexFactories: _*)
  }

  /**
    * get the currently registered [[MetadataFilterFactory]]-'s
    *
    * @return the currently registered [[MetadataFilterFactory]]-'s
    */
  def getCurrentMetadataFilterFactories(): scala.collection.immutable.Seq[MetadataFilterFactory] = {
    immutable.Seq(metadataFilterFactories: _*)
  }

  /**
    * get the currently registered [[ClauseTranslator]]-'s
    *
    * @return the currently registered [[ClauseTranslator]]-'s
    */
  def getCurrentClauseTranslators(): scala.collection.immutable.Seq[ClauseTranslator] = {
    immutable.Seq(clauseTranslators: _*)
  }

  /**
    * get the currently registered [[MetaDataTranslator]]-'s
    *
    * @return the currently registered [[MetaDataTranslator]]-'s
    */
  def getCurrentMetaDataTranslators(): scala.collection.immutable.Seq[MetaDataTranslator] = {
    immutable.Seq(metaDataTranslators: _*)
  }

  /**
    * gets the active [[MetadataStoreManagerType]]
    */
  def getActiveMetadataStoreManagerType(): MetadataStoreManagerType = {
    activeMetadataStoreManagerType
  }

  /**
    * gets the active [[MetadataStoreManager]]
    */
  def getActiveMetadataStoreManager(): MetadataStoreManager = {
    metaDataStoreManagers.get(activeMetadataStoreManagerType) match {
      case Some(metadataStoreManager) => metadataStoreManager
      // use the first default MetadataStoreManager
      case _ => metadataStoreManagersDefaults.head._2
    }
  }

  /**
    * resets all registrations to default
    */
  def reset(): Unit = {
    // currently mdstore cleanup handled in xskipper
    setDefaultRegistrations()
  }

  def setDefaultRegistrations(): Unit = {
    resetIndexFactoriesToDefault()
    resetMetadataFilterFactoriesToDefault()
    resetClauseTranslatorsToDefault()
    resetMetadataTranslatorsToDefault()
    resetActiveMetadataStoreManagerToDefault()
  }

  /**
    * Resets the registered [[IndexFactory]]'s to the default.
    */
  def resetIndexFactoriesToDefault(): Unit = {
    setIndexFactories(indexFactoriesDefaults)
  }

  /**
    * Sets the registered [[IndexFactory]]'s to be exactly the elements
    * of the given [[IndexFactory]] Seq.
    *
    * @param factories
    */
  def setIndexFactories(factories: Seq[IndexFactory]): Unit = {
    indexFactories.clear()
    // make sure to add only distinct values
    indexFactories ++= factories.toSet
  }

  /**
    * Resets the registered [[MetadataFilterFactory]]'s to the default.
    */
  def resetMetadataFilterFactoriesToDefault(): Unit = {
    setMetadataFilterFactories(metadataFilterFactoriesDefaults)
  }

  /**
    * Sets the registered [[MetadataFilterFactory]]'s to be exactly
    * the elements of the given [[MetadataFilterFactory]] Seq.
    *
    * @param factories
    */
  def setMetadataFilterFactories(factories: Seq[MetadataFilterFactory]): Unit = {
    metadataFilterFactories.clear()
    // make sure to add only distinct values
    metadataFilterFactories ++= factories.toSet
  }

  /**
    * Sets the registered [[MetadataStoreManager]]'s to be exactly
    * the elements of the given [[MetadataStoreManager]] map.
    *
    * @param metadataStoreManagers
    */
  def setMetadataStoreManagers(metadataStoreManagers: mutable.Map[MetadataStoreManagerType,
    MetadataStoreManager]): Unit = {
    metaDataStoreManagers.clear()
    metaDataStoreManagers ++= metadataStoreManagers
  }

  /**
    * Adds or replace a [[MetadataStoreManager]]
    *
    * @param metadataStoreManagerType the [[MetadataStoreManager]] type
    * @param metadataStoreManager the [[MetadataStoreManager]] associated with this type
    */
  def addOrReplaceMetadataStoreManager(metadataStoreManagerType: MetadataStoreManagerType,
                                       metadataStoreManager: MetadataStoreManager): Unit = {
    metaDataStoreManagers(metadataStoreManagerType) = metadataStoreManager
  }

  /**
    * Resets the registered [[ClauseTranslator]]'s to the default.
    */
  def resetClauseTranslatorsToDefault(): Unit = {
    setClauseTranslators(clauseTranslatorsDefaults)
  }

  /**
    * Sets the registered [[ClauseTranslator]]'s to be exactly the
    * elements of the given [[ClauseTranslator]] Seq.
    *
    * @param trasnlators
    */
  def setClauseTranslators(trasnlators: Seq[ClauseTranslator]): Unit = {
    clauseTranslators.clear()
    // make sure to add only distinct values
    clauseTranslators ++= trasnlators.toSet
  }

  /**
    * Resets the registered [[MetaDataTranslator]]'s to the default.
    */
  def resetMetadataTranslatorsToDefault(): Unit = {
    setMetaDataTranslators(metaDataTranslatorsDefaults)
  }

  /**
    * Sets the registered [[MetaDataTranslator]]'s to be exactly
    * the elements of the given [[MetaDataTranslator]] Seq.
    *
    * @param translators
    */
  def setMetaDataTranslators(translators: Seq[MetaDataTranslator]): Unit = {
    metaDataTranslators.clear()
    // make sure to add only distinct values
    metaDataTranslators ++= translators.toSet
  }

  /**
    * Reset the active [[MetadataStoreManager]] to default
    */
  def resetActiveMetadataStoreManagerToDefault(): Unit = {
    activeMetadataStoreManagerType = metadataStoreManagerTypeDefault
  }

  /**
    * Set the currently active [[MetadataStoreManager]]
    *
    * @param metadataStoreManagerType the type of the [[MetaDataStoreManagerType]] to be set
    */
  def setActiveMetadataStoreManager(metadataStoreManagerType: MetadataStoreManagerType): Unit = {
    if (metaDataStoreManagers.contains(metadataStoreManagerType)) {
      activeMetadataStoreManagerType = metadataStoreManagerType
      metaDataStoreManagers(metadataStoreManagerType).init()
    } else {
      logWarning(s"Unknown metadata store ${metadataStoreManagerType} falling to default")
      resetActiveMetadataStoreManagerToDefault()
      // Run setup
      metaDataStoreManagers(activeMetadataStoreManagerType).init()
    }
  }

  /**
    * Set the currently active the [[MetadataStoreManager]]
    * Used in Python module
    *
    * @param metadataStoreManager fully qualified name of MetadataStoreType to be used
    */
  def setActiveMetadataStoreManager(metadataStoreManager: String): Unit = {
    Utils.getObjectInstance[MetadataStoreManagerType](metadataStoreManager) match {
      case Some(metadataStoreManager) => setActiveMetadataStoreManager(metadataStoreManager)
      case _ => logWarning(s"Failed to set active MetadataStoreManager to ${metadataStoreManager}")
    }
  }

  /**
    * Adds a [[IndexFactory]] to the start of the [[IndexFactory]]-s list
    *
    * @param indexFactory the factory to add
    */
  def addIndexFactory(indexFactory: IndexFactory): Unit = {
    if (!indexFactories.contains(indexFactory)) {
      indexFactories.insert(0, indexFactory)
    }
  }

  /**
    * Adds a [[IndexFactory]] by class name
    *
    * @param indexFactoryClassName fully qualified name of the [[IndexFactory]] to add
    */
  def addIndexFactory(indexFactoryClassName: String): Unit = {
    Utils.getObjectInstance[IndexFactory](indexFactoryClassName) match {
      case Some(indexFactory) => addIndexFactory(indexFactory)
      case _ =>
    }
  }

  /**
    * Adds a [[MetadataFilterFactory]] to the end of the [[MetadataFilterFactory]]-s list
    *
    * @param metadataFilterFactory the factory to add
    */
  def addMetadataFilterFactory(metadataFilterFactory: MetadataFilterFactory): Unit = {
    if (!metadataFilterFactories.contains(metadataFilterFactory)) {
      metadataFilterFactories.append(metadataFilterFactory)
    }
  }

  /**
    * Adds a [[MetadataFilterFactory]] by class name
    * Used in Python module
    *
    * @param metadataFilterFactoryClassName the fully qualified name of
    *                                       [[MetadataFilterFactory]] to be used
    */
  def addMetadataFilterFactory(metadataFilterFactoryClassName: String): Unit = {
    Utils.getObjectInstance[MetadataFilterFactory](metadataFilterFactoryClassName) match {
      case Some(metadataFilterFactory) => addMetadataFilterFactory(metadataFilterFactory)
      case _ =>
    }
  }

  /**
    * Adds the given [[ClauseTranslator]] to the [[ClauseTranslator]-s Seq.
    * Addition is done at the beginning of the Seq to enable the client to override
    * the default translation.
    * Note that when translating the first translator which is able to translate
    * the clause is the one that will be used
    *
    * @param clauseTranslator the clause translator to add
    */
  def addClauseTranslator(clauseTranslator: ClauseTranslator): Unit = {
    if (!clauseTranslators.contains(clauseTranslator)) {
      clauseTranslators.insert(0, clauseTranslator)
    }
  }

  /**
    * Adds a [[ClauseTranslator]] by class name
    * Used in Python module
    *
    * @param clauseTranslatorClassName fully qualified name of [[ClauseTranslator]] to add
    */
  def addClauseTranslator(clauseTranslatorClassName: String): Unit = {
    Utils.getObjectInstance[ClauseTranslator](clauseTranslatorClassName) match {
      case Some(clauseTranslator) => addClauseTranslator(clauseTranslator)
      case _ =>
    }
  }

  /**
    * Adds the given [[MetaDataTranslator]] to the [[MetaDataTranslator]-s Seq.
    * Addition is done at the beginning of the Seq to enable the client
    * to override the default translation.
    * Note that when translating the first translator which is able to translate the metadata
    * is the one that will be used
    *
    * @param metaDataTranslator the translator to add
    */
  def addMetaDataTranslator(metaDataTranslator: MetaDataTranslator): Unit = {
    if (!metaDataTranslators.contains(metaDataTranslator)) {
      metaDataTranslators.insert(0, metaDataTranslator)
    }
  }

  /**
    * Adds a [[MetaDataTranslator]] by class name
    * Used in Python module
    *
    * @param metadataTranslatorClassName fully qualified name of [[MetaDataTranslator]] to add
    */
  def addMetaDataTranslator(metadataTranslatorClassName: String): Unit = {
    Utils.getObjectInstance[MetaDataTranslator](metadataTranslatorClassName) match {
      case Some(metaDataTranslator) => addMetaDataTranslator(metaDataTranslator)
      case _ =>
    }
  }
}
