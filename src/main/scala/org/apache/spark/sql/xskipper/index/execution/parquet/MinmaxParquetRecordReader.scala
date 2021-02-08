/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.spark.sql.xskipper.index.execution.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._

/**
  * Helper object to extract minimum and maximum statistics for a given column
  * from Parquet file footer
  */
object MinmaxParquetRecordReader extends Logging {

  private def getSchemaPathIndex(fileSchema: MessageType, filterPath: Array[String]): Int = {
    fileSchema.getPaths.indexWhere(_.sameElements(filterPath))
  }

  /**
    * Reading stats for a sequence of columns in parquet file
    *
    * @param path                the path of the parquet file
    * @param columns             the columns to read the stats for
    * @param hadoopConfiguration the hadoop configuration to be used when reading the file
    * @return The sequence of stats (min, max for each column) or None if the the column
    *         is not defined or no stats.
    *         for empty files null is returned for both min and max values
    */
  private def readStatFromParquet(
                                   path: String,
                                   columns: Seq[String],
                                   hadoopConfiguration: Configuration):
  Seq[Option[(_, _)]] = {
    val file = new Path(path)
    val footer = ParquetFileReader.readFooter(hadoopConfiguration, file)
    val reader = new ParquetFileReader(hadoopConfiguration, file, footer)

    // For empty parquet files return null as min/max for all columns
    if (reader.getRecordCount == 0) {
      return columns.map(_ => Some(null, null))
    }

    val fileSchema = footer.getFileMetaData.getSchema
    // Calculate statistics on all row groups if there are row groups or return None
    val rowGroups = reader.getRowGroups

    // No statistics in case column is not part of the file schema
    val stats = columns.map(col => {
      // Use path to support nested (GroupType)
      val filterPath: Array[String] = col.split("\\.")
      if (!fileSchema.containsPath(filterPath)) {
        logError("column " + col + " is not part of the file schema")
        None
      }
      else {
        val pathIndex = getSchemaPathIndex(fileSchema, filterPath)

        if (rowGroups.size() > 0) {
          val stat = reader.getRowGroups.get(0).getColumns.get(pathIndex).getStatistics

          reader.getRowGroups.foreach(block => {
            stat.mergeStatistics(block.getColumns.get(pathIndex).getStatistics)
          })
          logDebug("stat collected for col: " + columns + " " + stat)
          Some(stat)
        } else {
          None
        }
      }
    })

    stats.map(s => s match {
      // we can collect min/max only if the object contains non null values
      // Otherwise the min/max values will default to 0 which is not true
      case Some(stat) if stat.hasNonNullValue =>
        Some(stat.genericGetMin(), stat.genericGetMax())
      // column contain only null so we return null for both the min/max
      case Some(stat) if !stat.isEmpty =>
        Some(null, null)
      // This is an empty parquet file so store null as min/max
      case Some(null) => Some(null, null)
      // metadata doesn't contain statistics so we can't collect any metadata
      case _ =>
        None
    })
  }

  /**
    * Get the min and max numeric values of the given sequence of columns
    * in the parquet format file
    * by using parquet specific format - reading the statistics in the metadata blocks
    *
    * @param path    of parquet format file
    * @param columns the sequence of columns to return min and max values of
    * @return numeric min max values for each column
    *         (if the column doesn't exist or no min max values None is returned)
    */
  def getMinMaxStat(path: String, columns: Seq[String],
                    hadoopConfiguration: Configuration): Seq[Option[(_, _)]] = {
    readStatFromParquet(path, columns, hadoopConfiguration)
  }
}
