/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

import io.xskipper.index.Index
import io.xskipper.index.metadata.MetadataType
import io.xskipper.search.clause.Clause

object TranslationUtils {
  /**
    * Attempts to translate a given metadataType using the given a translators sequence by going
    * over the sequence and searching for the first available translation.
    * if no translation is found return None
    * @param metaDataStoreType the type of the metadatastore to which the metadataType
    *                          will be translated
    * @param metadataType the metadataType to be translated
    * @param index the index that created the metadataType
    * @param translators a sequence of MetaDataTranslator to be used when searching for translation
    * @tparam T the return type of the translation (property of the metaDataStoreType)
    * @return the metadataType translation or None if the given factories cannot translate
    *         the metadataType
    */
  def getMetaDataTypeTranslation[T](
                                     metaDataStoreType: MetadataStoreManagerType,
                                     metadataType: MetadataType,
                                     index: Index,
                                     translators: Seq[MetaDataTranslator]) : Option[T] = {
    // go over the translators and search for the first translation
    var res : Option[T] = None
    for (fac <- translators if res == None){
      res = fac.translate(metaDataStoreType, metadataType, index).asInstanceOf[Option[T]]
    }
    res
  }

  /**
    * Attempts to translate a given clause using the given a sequence of translators by going
    * over the sequence and searching for the first available translation.
    * if no translation is found return None
    *
    * @param metaDataStoreType the type of the metadatastore to which the clause will be translated
    * @param clause the clause to be translated
    * @param translators a sequence of ClauseTranslator to be used when searching for translation
    * @tparam T the return type of the translation (property of the metaDataStoreType)
    * @return the clause translation or None if the given factories cannot translate the clause
    */
  def getClauseTranslation[T](
                               metaDataStoreType: MetadataStoreManagerType,
                               clause: Clause,
                               translators: Seq[ClauseTranslator]) : Option[T] = {
    var res : Option[T] = None
    // go over the translators and search for the first translation
    for (fac <- translators if res == None){
      res = fac.translate(metaDataStoreType, clause, translators).asInstanceOf[Option[T]]
    }
    res
  }
}
