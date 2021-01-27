/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import java.util.Locale

import io.xskipper.index.metadata.MetadataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Base class for companion objects for indexes.
  * @tparam C the concrete Index subclass to which the companion
  *           pertains.
  */
abstract class IndexCompanion[C <: Index] {
  /**
    * Creates an index
    *
    * @param params parameters
    * @param keyMetadata key metadata
    * @param cols columns on which the index is to be created
    */
  def apply(params: Map[String, String], keyMetadata: Option[String], cols: Seq[String]) : C

  /**
    * Creates an index without any key metadata
    *
    * @param params parameters
    * @param cols columns on which the index is to be created
    * @return
    */
  def apply(params: Map[String, String], cols: Seq[String]) : C = apply(params, None, cols)
}

case class IndexField(name: String, dataType: DataType)

/**
  * Represents an abstract index for a metadata on a file.
  *
  * @param params a map containing the index parameters if exists
  * @param keyMetadata optional key metadata for encryption
  * @param cols a sequence of columns associated with the index
  */
abstract class Index(params: Map[String, String],
                     keyMetadata: Option[String],
                     cols: String*) extends Serializable with Logging {
  // make sure all cols are lower case
  val indexCols: Seq[String] = cols.map(_.toLowerCase(Locale.ROOT))
  // a mapping between the column name in lowercase to the struct field -
  // needed in order to get a field from a Row since Row columns are case sensitive
  var colsMap: Map[String, IndexField] = Map.empty
  // Indicate if metadata for this index should be generated
  // in an optimized way by receiving each object as a dataframe
  var isOptimized : Boolean = false


  /**
    * For some formats we might have an optimized way for collecting the metadata
    * This function enables this by receiving the entire file [[DataFrame]]
    * instead of processing it row by row
    * (For example in Parquet we can read the min/max from the footer)
    *
    * @param filePath the path of the file that is being processed
    * @param df a [[DataFrame]] with the file data
    * @param format the format of the file
    * @param options the options that were used to read the file
    * @return the collected MetadataType or null if no metadata was collected
    */
  def optCollectMetaData(filePath: String, df: DataFrame, format: String,
                         options: Map[String, String]): MetadataType = null

  /**
    * Gets a [[DataFrame]] row and extract the raw metadata needed by the index
    *
    * @param row [[Row]] a row to be indexed
    * @return raw metadata needed by the index or null if the row contain null value
    */
  def getRowMetadata(row: Row) : Any

  /**
    * @return the index params map
    */
  def getParams : Map[String, String] = params

  /**
    * @return the index columns (in lower case)
    */
  def getCols : Seq[String] = indexCols

  /**
    * @return the columns which the indexed is defined on
    */
  def getIndexCols: Iterable[IndexField] = colsMap.values

  /**
    * @return the name of the index
    */
  def getName: String

  /**
    * Given an accumulated metadata and new value - process the new value and returns an updated
    * accumulated metadata
    *
    * @param accuMetadata accumulated metadata created by processing all values until curr
    * @param curr new value to be processed
    * @return updated metadata for the index
    */
  def reduce(accuMetadata: MetadataType, curr: Any): MetadataType

  /**
   * Same as above reduce given two accumulated metadata
    *
   * @return updated metadata for the index
   */
  def reduce(md1: MetadataType, md2: MetadataType): MetadataType

  /**
    * @return "zero" value of the index - will be used for the first comparison to the object's
    *         rows data (by default this is null)
    */
  def generateBaseMetadata() : MetadataType = null

  /**
    * Gets a [[DataFrame]] and checks whether it is valid for the index
    * No need to check column existence as it is checked by the index builder
    *
    * @param df the [[DataFrame]] to be checked
    * @param schemaMap a map containing column names (as appear in the object) and their data types
    *                  the key is the column name in lower case
    * @throws [[XskipperException]] with the reason if invalid
    */
  def isValid(df: DataFrame, schemaMap: Map[String, (String, DataType)]) : Unit

  /**
    * Generate the column map according to a given schema
    *
    * @param schemaMap a map containing column names (as appear in the object) and their data types
    *                  the key is the column name in lower case
    */
  def generateColsMap(schemaMap: Map[String, (String, StructField)]): Unit = {
    colsMap ++= cols.map(col => {
      val field = schemaMap(col.toLowerCase(Locale.ROOT))
      (col -> IndexField(field._1, field._2.dataType))
    })
  }


  /**
    * @return the (full) name of the MetaDataType class used by this index
    */
  def getMetaDataTypeClassName() : String

  def getKeyMetadata() : Option[String] = {
    keyMetadata
  }

  def isEncrypted() : Boolean = {
    keyMetadata.isDefined
  }
}

