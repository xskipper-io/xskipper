/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index.execution.parquet

import io.xskipper.configuration.XskipperConf
import io.xskipper.index.Index
import io.xskipper.index.execution.PartitionSpec
import io.xskipper.index.metadata.MinMaxMetaData
import io.xskipper.metadatastore.MetadataHandle
import io.xskipper.utils.Utils
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{ByteType, DataType, ShortType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.SerializableConfiguration

/**
  * Helper object to extract minimum and maximum statistics for a given column
  * from Parquet file footer
  */
object ParquetMinMaxIndexing extends Logging{
  def parquetMinMaxParallelIndexing(
                                     metadataStore: MetadataHandle,
                                     partitionSpec: Option[PartitionSpec],
                                     options: Map[String, String],
                                     indexes: Seq[Index],
                                     fileIds: Seq[FileStatus],
                                     isRefresh: Boolean,
                                     spark: SparkSession): Unit = {
    val cols = indexes.flatMap(_.getIndexCols)
    val colsNames = cols.map(_.name)
    val dataTypes = cols.map(_.dataType)
    val numParallelism = Math.min(fileIds.size,
      XskipperConf.getConf(XskipperConf.XSKIPPER_PARQUET_MINMAX_OPTIMIZED_PARALLELISM))
    val serializableConfiguration =
      new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

    // create the metadata in parallel
    val metadataRDD = spark.sparkContext.parallelize(fileIds, numParallelism)
      .flatMap {
        case (fs) =>
          val parquetMetadata = getMinMaxStat(fs.getPath.toString,
            colsNames, serializableConfiguration)
          // store metadata only if it's defined for all of the columns collected
          // if it's not defined it means the parquet object didn't contain the metadata
          // and therefore we can't index this object using the optimized method
          if (parquetMetadata.forall(_.isDefined)) {
            val metadata = parquetMetadata.zipWithIndex.map {
                case(stat, i) =>
                  // create the min/max metadata
                  stat match {
                    case Some(values) =>
                      MinMaxMetaData(convert(values._1, dataTypes(i)),
                        convert(values._2, dataTypes(i)))
                    case _ => null
                  }
              }
            partitionSpec match {
              case Some(spec) => Some(Row.fromSeq(Seq(Utils.getFileId(fs),
                spec.values.toSeq(spec.schema)) ++ metadata))
              case _ => Some(Row.fromSeq(Seq(Utils.getFileId(fs)) ++ metadata))
            }
          } else {
            logWarning(s"Unable to index ${Utils.getFileId(fs)} using" +
              s" optimized min/max index collection for parquet")
            None
          }
      }

    partitionSpec match {
      case Some(spec) =>
        metadataStore.uploadMetadata(metadataRDD, Some(spec.schema), indexes, isRefresh)
      case _ =>
        metadataStore.uploadMetadata(metadataRDD, None, indexes, isRefresh)
    }
  }

  /**
    * Converts a given value according to the dataType
    * Needed since parquet saves the min/max for byte and short values as an int
    */
  def convert(v: Any, dataType: DataType): Any = {
    dataType match {
      case ShortType => v.asInstanceOf[Int].toShort
      case ByteType => v.asInstanceOf[Int].toByte
      case _ => v
    }
  }

  def getMinMaxStat(path: String, columns: Seq[String],
        hadoopSerializableConfiguration: SerializableConfiguration) : Seq[Option[(_, _)]] = {
    MinmaxParquetRecordReader.getMinMaxStat(path, columns, hadoopSerializableConfiguration.value)
  }
}
