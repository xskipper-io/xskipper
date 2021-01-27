/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search.filters

import io.xskipper.index.{BloomFilterIndex, Index, MinMaxIndex, ValueListIndex}

trait MetadataFilterFactory {
  /**
    * Create [[MetaDataFilters]] according to given indexes
    *
    * @param descriptors the sequence of indexes to be processed
    * @return a sequence of MetaDataFilter matching the given indexes
    */
  def getFilters(descriptors : Seq[Index]) : Seq[MetadataFilter]
}

object BaseFilterFactory extends MetadataFilterFactory {
  /**
    * Create MetaDataFilters according to given indexes
    *
    * @param indexes the sequence of indexes to be processed
    * @return a sequence of MetaDataFilter matching the given indexes
    */
  override def getFilters(indexes: Seq[Index]): Seq[MetadataFilter] = {
    indexes.flatMap(descriptor => descriptor match {
      case MinMaxIndex(col, _) => Some(MinMaxFilter(col))
      case ValueListIndex(col, _) => Some(ValueListFilter(col))
      case BloomFilterIndex(col, _, _, _) => Some(BloomFilterFilter(col))
      case _ => None
    })
  }
}
