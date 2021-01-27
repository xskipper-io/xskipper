/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import java.util.Locale

import io.xskipper.XskipperException
import io.xskipper.configuration.{ConfigurationUtils, XskipperConf}
import io.xskipper.index.metadata.{BloomFilterMetaData, MetadataType}
import io.xskipper.status.Status
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.sketch.BloomFilter


object BloomFilterIndex extends IndexCompanion[BloomFilterIndex] {
  override def apply(params: Map[String, String], keyMetadata: Option[String],
                     cols: Seq[String]): BloomFilterIndex = {
    if (cols.size != 1) {
      throw new XskipperException("BloomFilter Index takes exactly 1 Column!")
    }
    val col = cols(0)
    val fpp = ConfigurationUtils.getOrElseFromMap(XskipperConf.BLOOM_FILTER_FPP_KEY,
      XskipperConf.BLOOM_FILTER_FPP.defaultValue, params)

    val ndv = ConfigurationUtils.getOrElseFromMap(XskipperConf.BLOOM_FILTER_NDV_KEY,
      XskipperConf.BLOOM_FILTER_NDV.defaultValue, params)
    BloomFilterIndex(col, fpp, ndv, keyMetadata)
  }
}
/**
  * Represents an abstract bloom filter index
  *
  * @param col the column on which the index is applied
  * @param fpp the expected false positive probability of the bloom filter
  * @param ndv the expected number of distinct values
  * @param keyMetadata optional key metadata
  */
case class BloomFilterIndex(col : String,
                            fpp: Double = XskipperConf.getConf(XskipperConf.BLOOM_FILTER_FPP),
                            ndv: Long = XskipperConf.getConf(XskipperConf.BLOOM_FILTER_NDV),
                            keyMetadata: Option[String] = None)
  extends Index(Map(XskipperConf.BLOOM_FILTER_FPP_KEY -> fpp.toString,
    XskipperConf.BLOOM_FILTER_NDV_KEY -> ndv.toString), keyMetadata, col) {
  override def getName: String = "bloomfilter"

  override def getMetaDataTypeClassName(): String = classOf[BloomFilterMetaData].getName

  override def getRowMetadata(row: Row): Any = {
    row.get(row.fieldIndex(colsMap(col).name))
  }

  override def reduce(md1: MetadataType, md2: MetadataType): MetadataType = {
    (md1, md2) match {
      case (null, null) => null
      case (md1: MetadataType, null) => md1
      case (null, md2: MetadataType) => md2
      case (md1: MetadataType, md2: MetadataType) =>
        // merge bloom filter
        md1.asInstanceOf[BloomFilterMetaData].bloomFilter
          .mergeInPlace(md2.asInstanceOf[BloomFilterMetaData].bloomFilter)
        md1
    }
  }

  override def reduce(accuMetadata: MetadataType, curr: Any): MetadataType = {
    (accuMetadata, curr) match {
      case (null, null) => BloomFilterMetaData(BloomFilter.create(ndv, fpp))
      case (accuMetadata: MetadataType, null) => accuMetadata
      case (null, curr: Any) =>
        val res = BloomFilterMetaData(BloomFilter.create(ndv, fpp))
        // add value to bloom filter
        updater(res.bloomFilter, curr)
        res
      case (accuMetadata: MetadataType, curr: Any) =>
        // add value to bloom filter
        updater(accuMetadata.asInstanceOf[BloomFilterMetaData].bloomFilter, curr)
        accuMetadata
    }
  }

  private def updater(filter: BloomFilter, curr: Any): Unit = curr match {
    case s: String => filter.putString(s)
    case b: Byte => filter.putLong(b)
    case s: Short => filter.putLong(s)
    case i: Int => filter.putLong(i)
    case l: Long => filter.putLong(l)
    case _ =>
      throw new IllegalArgumentException(
        s"Bloom filter only supports string type and integral types, " +
          s"and does not support type ${colsMap(col).dataType}"
      )
  }

  /**
    * Gets a [[DataFrame]] and checks whether it is valid for the index
    *
    * @param df the [[DataFrame]] to be checked
    * @param schemaMap a map containing column names (as appear in the object) and their data types
    *                  the key is the column name in lower case
    * @throws [[XskipperException]] if invalid index
    */
  override def isValid(df: DataFrame,
                       schemaMap: Map[String, (String, DataType)]) : Unit = {
    if (!(schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[StringType] ||
      // spark bloom filter supports IntegralTypes
      // since the class is a private class we enumerate here all of the options
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[ByteType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[LongType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[IntegerType] ||
      schemaMap(col.toLowerCase(Locale.ROOT))._2.isInstanceOf[ShortType]
    )) {
      throw new XskipperException(Status.BLOOMFILTER_WRONG_TYPE)
    }
  }
}
