# Copyright 2021 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql.dataframe import DataFrame
from py4j.java_collections import MapConverter

class IndexBuilder:
    """
    Helper class for building indexes

    :param sparkSession: SparkSession object
    :param uri: the URI of the dataset / the identifier of the hive table on which the index is defined
    :param xskipper: the xskipper instance associated with this IndexBuilder
    """
    def __init__(self, spark, uri, xskipper):
        self._jindexBuilder = spark._jvm.io.xskipper.index.execution.IndexBuilder(spark._jsparkSession, uri,
                                                                             xskipper.xskipper)
        self.xskipper = xskipper
        self.spark = spark

    def addMinMaxIndex(self, col, keyMetadata=None):
        """
        Adds a MinMax index for the given column

        :param col: the column to add the index on
        :param keyMetadata: optional key metadata
        """
        if keyMetadata:
            self._jindexBuilder.addMinMaxIndex(col, keyMetadata)
        else:
            self._jindexBuilder.addMinMaxIndex(col)
        return self

    def addValueListIndex(self, col, keyMetadata=None):
        """
        Adds a ValueList index on the given column

        :param col: the column to add the index on
        :param keyMetadata: optional key metadata
        """
        if keyMetadata:
            self._jindexBuilder.addValueListIndex(col, keyMetadata)
        else:
            self._jindexBuilder.addValueListIndex(col)
        return self

    def addBloomFilterIndex(self, col, keyMetadata=None):
        """
        Adds a BloomFilter index on the given column

        :param col: the column to add the index on
        :param keyMetadata: optional key metadata
        """
        if keyMetadata:
            self._jindexBuilder.addBloomFilterIndex(col, keyMetadata)
        else:
            self._jindexBuilder.addBloomFilterIndex(col)
        return self

    def addCustomIndex(self, indexClass, cols, params, keyMetadata=None):
        """
        Adds a Custom index on the given columns

        :param cols: a sequence of cols
        :param params: a map of index specific parameters
        :param keyMetadata: optional key metadata
        """
        gateway = self.spark.sparkContext._gateway
        jmap = MapConverter().convert(params, gateway._gateway_client)
        objCls = gateway.jvm.String
        colsArr = gateway.new_array(objCls, len(cols))
        for i in range(len(cols)):
            colsArr[i] = cols[i]
        if keyMetadata:
            self._jindexBuilder.addCustomIndex(indexClass, colsArr, jmap, keyMetadata)
        else:
            self._jindexBuilder.addCustomIndex(indexClass, colsArr, jmap)
        return self

    def build(self, reader=None):
        """
        Builds the index

        :param dataFrameReader: if uri in the xskipper instance is a table identifier \
                                a DataFrameReader instance to enable reading the URI as a DataFrame
                                Note: The reader is assumed to have all of the parameters configured.
                                `reader.load(Seq(<path>))` will be used by the indexing code to read each
                                object separately
        :return: dataFrame object containing statistics about the build operation
        """
        if reader:
            return DataFrame(self._jindexBuilder.build(reader._jreader), self.spark)
        else:
            # build for tables
            return DataFrame(self._jindexBuilder.build(), self.spark)
