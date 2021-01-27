/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index

import io.xskipper.configuration.XskipperConf

trait IndexFactory {
  /**
    * Create an [[Index]] instance according to the given parameters
    *
    * @param indexType the index type to be created
    * @param cols the columns on which the index should be created
    * @param params the index parameters map
    * @param keyMetadata the key metadata associated with the index
    * @return The index that can be created from the parameters if possible
    */
  def getIndex(indexType: String, cols: Seq[String], params: Map[String, String],
               keyMetadata: Option[String] = None) : Option[Index]
}

object IndexFactoryUtils {
  /**
    * Attempts to create the an [[Index]] instance from the given parameters
    * using the given list of [[IndexFactory]]-s
    * The result will be that of the first factory that can process the given
    * parameters if no translation is found return None
    *
    * @param indexType the index type to be created
    * @param cols the columns on which the index should be created
    * @param params the index parameters map
    * @param keyMetadata the key metadata associated with the index
    * @param indexFactories the list of factories to be used in index creation
    * @return The index that can be created from the parameters if possible
    */
  def getIndex(indexType: String,
                  cols: Seq[String],
                  params: Map[String, String],
                  keyMetadata: Option[String],
                  indexFactories: Seq[IndexFactory]) : Option[Index] = {
    // go over the factories and get the first factory which can create an index from
    // the given parameters
    var res : Option[Index] = None
    for (fac <- indexFactories if res == None){
      res = fac.getIndex(indexType, cols, params, keyMetadata).asInstanceOf[Option[Index]]
    }
    res
  }
}

object BaseIndexFactory extends IndexFactory {
  /**
    * Create an [[Index]] instance according to the given parameters
    *
    * @param indexType the index type to be created
    * @param cols the columns on which the index should be created
    * @param params the index parameters map
    * @param keyMetadata the key metadata associated with the index
    * @return The index that can be created from the parameters if possible
    */
  override def getIndex(indexType: String, cols: Seq[String], params: Map[String, String],
                        keyMetadata: Option[String] = None) : Option[Index] = {
    indexType match {
      case "minmax" => Some(MinMaxIndex(cols(0), keyMetadata))
      // MinMax index support only one column
      case "valuelist" => Some(ValueListIndex(cols(0), keyMetadata))
      // ValueList index support only one column
      case "bloomfilter" =>
        val fppOpt: Option[Double] = params.get(XskipperConf.BLOOM_FILTER_FPP_KEY) match {
          case Some(fpp) => Some(fpp.toDouble)
          case _ if params.contains(XskipperConf.LEGACY_BLOOM_FILTER_FPP_KEY) =>
            Some(params.get(XskipperConf.LEGACY_BLOOM_FILTER_FPP_KEY).get.toDouble)
          case _ => None
        }
        val ndvOpt : Option[Double] = params.get(XskipperConf.BLOOM_FILTER_NDV_KEY) match {
          case Some(fpp) => Some(fpp.toDouble)
          case _ if params.contains(XskipperConf.LEGACY_BLOOM_FILTER_NDV_KEY) =>
            Some(params.get(XskipperConf.LEGACY_BLOOM_FILTER_NDV_KEY).get.toDouble)
          case _ => None
        }
        (fppOpt, ndvOpt) match {
          case (Some(fpp), Some(ndv)) => Some(BloomFilterIndex(cols(0), fpp.toDouble, ndv.toLong))
          // backward compatibility
          case (Some(fpp), None) => Some(BloomFilterIndex(cols(0), fpp.toDouble,
            keyMetadata = keyMetadata))
          // Use the default precision and ndv
          case _ => Some(BloomFilterIndex(cols(0), keyMetadata = keyMetadata))
        }
      case _ => None
    }
  }
}
