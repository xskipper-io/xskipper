/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

import io.xskipper.index.Index
import io.xskipper.index.metadata.MetadataType

trait MetaDataTranslator extends Serializable {
  /**
    * Translates a metaDataType to the metaDataStore representation
    *
    * @param metadataStoreManagerType the [[MetadataStoreManagerType]] to translate to
    * @param metadataType the [[MetadataType]] to translate
    * @param index the index that created [[MetadataType]]
    * @return return type is Any since each metaDataStore may require different representation
    */
  def translate(metadataStoreManagerType: MetadataStoreManagerType,
                metadataType: MetadataType, index: Index) : Option[Any]
}
