# Copyright 2021 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

class Registration:
    """
    Helper class for registering Factories and Translators for xskipper plugins
    """

    @staticmethod
    def addMetadataFilterFactory(sparkSession, filterFactory):
        """
        Adds a MetadataFilterFactory to the end of the MetadataFilterFactory-s list

        :param sparkSession: SparkSession object
        :param filterFactory: fully qualified name of MetadataFilterFactory to be used
        """
        sparkSession._jvm.io.xskipper.Registration.addMetadataFilterFactory(filterFactory)

    @staticmethod
    def addIndexFactory(sparkSession, indexFactory):
        """
        Adds a IndexFactory to the start of the IndexFactory-s list

        :param sparkSession: SparkSession object
        :param indexFactory: fully qualified name of IndexFactory to be used
        """
        sparkSession._jvm.io.xskipper.Registration.addIndexFactory(indexFactory)

    @staticmethod
    def addMetaDataTranslator(sparkSession, metadataTranslator):
        """
        Adds a IndexFactory to the start of the IndexFactory-s list

        :param sparkSession: SparkSession object
        :param metadataTranslator: fully qualified name of MetadataTranslator to be used
        """
        sparkSession._jvm.io.xskipper.Registration.addMetaDataTranslator(metadataTranslator)

    @staticmethod
    def addClauseTranslator(sparkSession, clauseTranslator):
        """
        Adds a ClauseTranslator to the start of the ClauseTranslator-s list

        :param sparkSession: SparkSession object
        :param clauseFactories: fully qualified name of ClauseTranslator to be used
        """
        sparkSession._jvm.io.xskipper.Registration.addClauseTranslator(clauseTranslator)

    @staticmethod
    def setActiveMetadataStoreManager(sparkSession, metadataStoreManager):
        """
        Set the currently active the [[MetadataStoreManager]]

        :param sparkSession: SparkSession object
        :param metadataStoreManager: fully qualified name of MetadataStoreManagerType to be used
        """
        sparkSession._jvm.io.xskipper.Registration.setActiveMetadataStoreManager(metadataStoreManager)

    @staticmethod
    def getActiveMetadataStoreManagerType(sparkSession):
        """
        Returns the fully qualified name of the active MetadataStoreManagerType

        :param sparkSession: SparkSession object
        :return: fully qualified name of the active MetadataStoreManagerType
        """
        return sparkSession._jvm.io.xskipper.Registration.getActiveMetadataStoreManagerType().getClass().getCanonicalName()
