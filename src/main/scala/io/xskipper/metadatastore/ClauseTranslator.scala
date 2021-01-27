/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

import io.xskipper.search.clause.Clause

/**
  * Note: it is the responsibility of the translation to take into account
  * the metadata store specific properties
  */
trait ClauseTranslator {
  /**
    * Translates a clause to the representation of a given MetaDataStoreType
    *
    * @param metadataStoreManagerType the [[MetadataStoreManagerType]] to translate to
    * @param clause the clause to be translated
    * @param clauseTranslators a sequence of clause translators to enable recursive translation
    * @return return type is Any since each metaDataStore may require different representation
    */
  def translate(metadataStoreManagerType: MetadataStoreManagerType,
                clause: Clause, clauseTranslators: Seq[ClauseTranslator]) : Option[Any]
}
