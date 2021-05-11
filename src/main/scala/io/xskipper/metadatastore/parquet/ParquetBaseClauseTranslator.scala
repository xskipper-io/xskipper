/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import io.xskipper.index.metadata.BloomFilterMetaData
import io.xskipper.metadatastore.{ClauseTranslator, MetadataStoreManagerType, TranslationUtils}
import io.xskipper.search.clause._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.IsNotNull
import org.apache.spark.sql.types.MetadataTypeUDT

object ParquetBaseClauseTranslator extends ClauseTranslator {
  /**
    * Translates a clause to the representation of Parquet [[MetaDataStoreType]]
    *
    * @param metadataStoreManagerType the [[MetadataStoreManagerType]] to translate to
    * @param clause the clause to be translated
    * @param clauseTranslators a sequence of clause translation factories
    *                          to enable recursive translation
    * @return return type is Any since each metadatastore may require different representation
    */
  override def translate(metadataStoreManagerType: MetadataStoreManagerType, clause: Clause,
                         clauseTranslators: Seq[ClauseTranslator]): Option[Column] = {
    metadataStoreManagerType match {
      case Parquet =>
        clause match {
          case AndClause(left, right) =>
            (TranslationUtils.getClauseTranslation[Column](Parquet, left, clauseTranslators),
             TranslationUtils.getClauseTranslation[Column](Parquet, right, clauseTranslators))
            match {
              case (Some(leftQuery), Some(rightQuery)) => Some(leftQuery.and(rightQuery))
              case _ => None
            }
          case OrClause(left, right) =>
            (TranslationUtils.getClauseTranslation[Column](Parquet, left, clauseTranslators),
             TranslationUtils.getClauseTranslation[Column](Parquet, right, clauseTranslators))
            match {
              case (Some(leftQuery), Some(rightQuery)) => Some(leftQuery.or(rightQuery))
              case _ => None
            }
          case NotClause(c) =>
            TranslationUtils.getClauseTranslation[Column](Parquet, c, clauseTranslators) match {
              case Some(query) => Some(!query)
              case _ => None
            }
          case TrueClause(c) =>
            Some(lit(true))
          case FalseClause(c) =>
            Some(lit(false))
          case MinMaxClause(column, op, value, isMin) =>
            val mdColName = ParquetUtils.getColumnNameForCols(Seq(column), "minmax")
            val metadataCol = isMin match {
              case true => col(s"${mdColName}.min")
              case _ => col(s"${mdColName}.max")
            }
            // the assumption here is that if the metadata column contains null values
            // it means the data contain only nulls and since Spark inserts isNotNull to the
            // condition it will work fine
            val expression = op match {
              case GT => metadataCol > value
              case GTE => metadataCol >= value
              case LT => metadataCol < value
              case LTE => metadataCol <= value
            }
            Some(!isnull(metadataCol).and(expression))
          // checks if a list of values exists in the value list metadata
          // (used for equality checks)
          case ValueListClause(column, values, false) =>
            val mdColName = ParquetUtils.getColumnNameForCols(Seq(column), "valuelist")
            Some((!isnull(col(mdColName))).and(arrays_overlap(col(mdColName), lit(values))))
          // checks if the value list metadata contain values which
          // are different than the given list of values
          // (used for inequality checks)
          case ValueListClause(column, values, true) =>
            val mdColName = ParquetUtils.getColumnNameForCols(Seq(column), "valuelist")
            Some((!isnull(col(mdColName))).and(size(array_except(col(mdColName), lit(values))) > 0))
          case BloomFilterClause(column, values) =>
            val mdColName = ParquetUtils.getColumnNameForCols(Seq(column), "bloomfilter")
            Some(bloomFilterUDF(values)(col(mdColName)))
          case _ =>
            None
        }
      case _ => None
    }
  }

  class BloomFilterMetaDataTypeUDT extends MetadataTypeUDT[BloomFilterMetaData]
  /**
    * BloomFilterMetaData udf function for querying if value exists
    * in indexed column using bloom filter
    *
    * @return true if the given the column should be read given the metadata
    */
  def bloomFilterUDF(values: Seq[Literal]): UserDefinedFunction = {
    // BloomFilter does not support Literal type so convert to scala type
    // In addition having a valid type is enforced by the filter and the index creation
    val scalaValues = values.map(v => CatalystTypeConverters.convertToScala(v.value, v.dataType))
    udf((col: BloomFilterMetaData) => {
      col == null ||
        (col.bloomFilter != null &&
          scalaValues.exists(v => col.bloomFilter.mightContain(v)))
    })
  }
}
