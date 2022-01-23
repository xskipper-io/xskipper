/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing.util

import java.io.File

import io.xskipper.index.execution.IndexBuilder
import io.xskipper.status.QueryIndexStatsResult
import io.xskipper.{Registration, Xskipper}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import java.net.URI

object Utils {

  // defaults for CSV
  val CSV_DEFAULT_SUFFIX: String = ".csv"
  val CSV_DEFAULT_READER_OPTIONS: Map[String, String] =
    Map[String, String]("header" -> "true", "inferSchema" -> "true")
  val CSV_DEFAULT_WRITER_OPTIONS: Map[String, String] = Map[String, String]("header" -> "true")

  // default for JSON
  val JSON_DEFAULT_SUFFIX: String = ".json"
  val JSON_DEFAULT_READER_OPTIONS: Map[String, String] =
    Map[String, String]("inferSchema" -> "true")
  val JSON_DEFAULT_WRITER_OPTIONS: Map[String, String] = Map.empty[String, String]

  // defaults for Parquet
  val PARQUET_DEFAULT_SUFFIX: String = ".snappy.parquet"
  val PARQUET_DEFAULT_READER_OPTIONS: Map[String, String] = Map.empty[String, String]
  val PARQUET_DEFAULT_WRITER_OPTIONS: Map[String, String] = Map.empty[String, String]

  // defaults for ORC
  val ORC_DEFAULT_SUFFIX: String = ".snappy.orc"
  val ORC_DEFAULT_READER_OPTIONS: Map[String, String] = Map.empty[String, String]
  val ORC_DEFAULT_WRITER_OPTIONS: Map[String, String] = Map.empty[String, String]


  val formatsToDefaultSuffixMap: CaseInsensitiveMap[String] = CaseInsensitiveMap(Map(
    "csv" -> CSV_DEFAULT_SUFFIX,
    "json" -> JSON_DEFAULT_SUFFIX,
    "parquet" -> PARQUET_DEFAULT_SUFFIX,
    "orc" -> ORC_DEFAULT_SUFFIX
  ))

  val formatsToDefaultReaderOptionsMap: CaseInsensitiveMap[Map[String, String]] =
    CaseInsensitiveMap(Map(
      "csv" -> CSV_DEFAULT_READER_OPTIONS,
      "json" -> JSON_DEFAULT_READER_OPTIONS,
      "parquet" -> PARQUET_DEFAULT_READER_OPTIONS,
      "orc" -> ORC_DEFAULT_READER_OPTIONS
    ))

  val formatsToDefaultWriterOptionsMap: CaseInsensitiveMap[Map[String, String]] =
    CaseInsensitiveMap(Map(
      "csv" -> CSV_DEFAULT_WRITER_OPTIONS,
      "json" -> JSON_DEFAULT_WRITER_OPTIONS,
      "parquet" -> PARQUET_DEFAULT_WRITER_OPTIONS,
      "orc" -> ORC_DEFAULT_WRITER_OPTIONS
    ))

  def getReaderOptions(format: String): Map[String, String] = {
    formatsToDefaultReaderOptionsMap.get(format) match {
      case Some(res) => res
      case None => throw new IllegalArgumentException(s"Format $format is unknown")
    }
  }

  def getWriterOptions(format: String): Map[String, String] = {
    formatsToDefaultWriterOptionsMap.get(format) match {
      case Some(res) => res
      case None => throw new IllegalArgumentException(s"Format $format is unknown")
    }
  }

  def getDefaultSuffix(format: String): String = {
    formatsToDefaultSuffixMap.get(format) match {
      case Some(res) => res
      case None => throw new IllegalArgumentException(s"Format $format is unknown")
    }
  }


  def isResDfValid(df: DataFrame, expectedNumIndexedFiles: Option[Int] = None,
                   expectedNumRemovedFiles: Option[Int] = None): Boolean = {
    val status = df.select("status").rdd.map(r => r(0)).collect()
    expectedNumIndexedFiles match {
      case Some(n) =>
        val numIndexedFiles = df.select("new_entries_added")
          .rdd.map(r => r(0)).collect().head.asInstanceOf[Int]
        assert(numIndexedFiles == n)
      case _ =>
    }
    expectedNumRemovedFiles match {
      case Some(n) =>
        val numRemovedFiles = df.select("old_entries_removed")
          .rdd.map(r => r(0)).collect().head.asInstanceOf[Int]
        assert(numRemovedFiles == n)
      case _ =>
    }
    status.length == 1 && status.head.asInstanceOf[String] == "SUCCESS"
  }

  def getIndexBuilder(xskipper: Xskipper, indexTypes: Seq[(String, Seq[String])]): IndexBuilder = {
    indexTypes.foldLeft(xskipper.indexBuilder())(
      { case (ib: IndexBuilder, (idxType: String, cols: Seq[String])) =>
        idxType match {
          case "minmax" if cols.size == 1 =>
            ib.addMinMaxIndex(cols.head)
          case "valuelist" if cols.size == 1 =>
            ib.addValueListIndex(cols.head)
          case "bloomfilter" if cols.size == 1 =>
            ib.addBloomFilterIndex(cols.head)
          case _ =>
            throw new IllegalStateException(s"Index type $idxType with cols $cols is illegal")
        }
      })
  }

  def concatPaths(parent: String, child: String): String = {
    val relChild = new URI("/").relativize(new URI(child)).toString
    new Path(new Path(parent), new Path(relChild)).toString
  }

  def readTextFileAsString(fileName: String): String = {
    val source = scala.io.Source.fromFile(fileName)
    val res = source.mkString
    source.close()
    res
  }

  def getDefaultReader(spark: SparkSession, format: String): DataFrameReader = {
    spark.read.format(format).options(getReaderOptions(format))
  }

  implicit def strToPath(pathStr: String): Path = new Path(pathStr)

  implicit def strToFile(pathStr: String): File = new File(pathStr)

  implicit def pathToFile(path: Path): File = new File(path.toUri)

  // get the number of skipped objects for the latest query
  def getNumSkippedFilesAndClear(): Long = {
    val aggregatedStats: QueryIndexStatsResult =
      Registration.getActiveMetadataStoreManager().getLatestQueryAggregatedStats()
    // reset for next test
    Xskipper.clearStats()
    aggregatedStats.skipped_Objs
  }


  def getResultSet(basePath: String, filesToSkip: String*): Set[String] = {
    filesToSkip.map(concatPaths(basePath, _)).toSet
  }
}
