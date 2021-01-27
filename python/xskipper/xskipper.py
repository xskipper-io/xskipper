# Copyright 2021 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

import py4j
from py4j.java_collections import MapConverter
from pyspark.sql.dataframe import DataFrame

from .indexbuilder import IndexBuilder


class Xskipper:
    """
    Main class for programmatically interacting with xskipper

    :param sparkSession: SparkSession object
    :param uri: the URI of the dataset / the identifier of the hive table on which the index is defined
    :param metadataStoreManagerClassName: fully qualified name of MetadataStoreManager to be used
    """

    def __init__(self, sparkSession, uri, metadataStoreManagerClassName=None):
        self.spark = sparkSession
        self.uri = uri
        if metadataStoreManagerClassName:
            self.xskipper = self.spark._jvm.io.xskipper.Xskipper(self.spark._jsparkSession, uri, metadataStoreManagerClassName)
        else:
            self.xskipper = self.spark._jvm.io.xskipper.Xskipper(self.spark._jsparkSession, uri,
                                                                 'io.xskipper.metadatastore.parquet.ParquetMetadataStoreManager')

    def dropIndex(self):
        """
        Deletes the index

        :raises XskipperException: if index cannot be removed
        """
        self.xskipper.dropIndex()

    def refreshIndex(self, dataFrameReader=None):
        """
        Refresh the index

        :param dataFrameReader: if uri is a table identifier
                                a DataFrameReader instance to enable reading the URI as a DataFrame
                                Note: The reader is assumed to have all of the parameters configured.
                                `reader.load(Seq(<path>))` will be used by the indexing code to read each
                                object separately
        :return: dataFrame object containing statistics about the refresh operation
        :raises XskipperException: if index cannot be refreshed
        """
        if dataFrameReader:
            return DataFrame(self.xskipper.refreshIndex(dataFrameReader._jreader), self.spark._wrapped)
        else:
            # refresh for tables
            return DataFrame(self.xskipper.refreshIndex(), self.spark._wrapped)

    def isIndexed(self):
        """
        Checks if the URI is indexed

        :return: true if the URI is indexed
        """
        return self.xskipper.isIndexed()

    def indexBuilder(self):
        """
        Helper class for setting and building an index

        :return: IndexBuilder: instance
        """
        return IndexBuilder(self.spark, self.uri, self)

    def describeIndex(self, dataFrameReader=None):
        """
        Describes the indexes on the URI (for non table URI)

        :param dataFrameReader: if uri is a table identifier
                                a DataFrameReader instance to enable reading the URI as a DataFrame
        :return: dataFrame object containing information about the index
        :raises XskipperException: if index cannot be refreshed
        """
        if dataFrameReader:
            return DataFrame(self.xskipper.describeIndex(dataFrameReader._jreader), self.spark._wrapped)
        else:
            return DataFrame(self.xskipper.describeIndex(), self.spark._wrapped)

    def getLatestQueryStats(self):
        """
        Return latest query skipping statistics for this xskipper instance

        In case the API was called on a URI without an index or the API was called without
        running a query the returned DataFrame structure is - status, reason with status=FAILED
        In case the query cannot be skipped because one of the following:

        1. Dataset has no indexed files

        2. No query to the metadata store can be generated - can be due to a predicate that can not
            be used in skipping (or maybe due to missing metadata filter) or due to failure to
            translate the abstract query.
            the returned dataframe structure is:
            status, isSkippable, skipped_Bytes, skipped_Objs, total_Bytes,
            total_Objs with status=SUCCESS, isSkippable=false and all other values are -1
            Otherwise the dataframe structure is the same as above with isSkippable=true
            and the relevant stats

        :return: object containing information about latest query stats
        """
        return DataFrame(self.xskipper.getLatestQueryStats(), self.spark._wrapped)

    def setParams(self, params):
        """
        Update instance specific MetadataHandle parameters

        :param params: a map of parameters to be set
        """
        gateway = self.spark.sparkContext._gateway
        jmap = MapConverter().convert(params, gateway._gateway_client)
        self.xskipper.setParams(jmap)

    @staticmethod
    def setConf(sparkSession, params):
        """
        Updates JVM wide xskipper parameters (Only given parameters will be updated)

        :param sparkSession: SparkSession object
        :param params: a map of parameters to be set
        """
        gateway = sparkSession.sparkContext._gateway
        jmap = MapConverter().convert(params, gateway._gateway_client)
        sparkSession._jvm.io.xskipper.Xskipper.setConf(jmap)

    @staticmethod
    def getConf(sparkSession):
        """
        Returns a map of all configurations currently set

        :param sparkSession: SparkSession object
        :return: a map of the current values currently set in the configuration
        """
        return sparkSession._jvm.io.xskipper.Xskipper.getConf()

    @staticmethod
    def set(sparkSession, key, value):
        """
        Sets a specific key in the JVM wide configuration

        :param sparkSession: SparkSession object
        :param key: the key to set
        :param value: the value associated with the key
        """
        sparkSession._jvm.io.xskipper.Xskipper.set(key, value)

    @staticmethod
    def unset(sparkSession, key):
        """
        Removes a key from the configuration

        :param sparkSession: SparkSession object
        :param key: the key to remove
        """
        sparkSession._jvm.io.xskipper.Xskipper.unset(key)

    @staticmethod
    def get(sparkSession, key):
        """
        Retrieves the value associated with the given key in the configuration

        :param sparkSession: SparkSession object
        :param key: the key to lookup
        :return: the value associated with the key or None if the key doesn't exist
        """
        return sparkSession._jvm.io.xskipper.Xskipper.get(key)

    @staticmethod
    def getLatestQueryAggregatedStats(sparkSession):
        """
        Gets the aggregated latest query skipping stats for all active MetadataHandle instances
        in the current default MetadataStoreManager.
        In order to get reliable results it is assumed that either clearStats
        or clearActiveMetadataHandles was called before running the query.

        In case the API was called on a URI without an index or the API was called without
        running a query the returned DataFrame structure is - status, reason with status=FAILED
        In case the query cannot be skipped because one of the following:

        1. Dataset has no indexed files

        2. No query to the metadata store can be generated - can be due to a predicate that can not
           be used in skipping (or maybe due to missing metadata filter) or due to failure to
           translate the abstract query.
           the returned dataframe structure is:
           status, isSkippable, skipped_Bytes, skipped_Objs, total_Bytes,
           total_Objs with status=SUCCESS, isSkippable=false and all other values are -1
           Otherwise the dataframe structure is the same as above with isSkippable=true
           and the relevant stats

        :param sparkSession: SparkSession object
        :return: dataFrame object containing information about latest query stats
        """
        return DataFrame(sparkSession._jvm.io.xskipper.Xskipper.getLatestQueryAggregatedStats(
            sparkSession._jsparkSession), sparkSession._wrapped)

    @staticmethod
    def clearStats(sparkSession):
        """
        Clears the stats for all active MetadataHandle instances in the active MetadataStoreManager
        Should be called before each query to make sure the aggregated stats are cleared

        :param sparkSession: SparkSession object
        """
        sparkSession._jvm.io.xskipper.Xskipper.clearStats()

    @staticmethod
    def reset(sparkSession):
        """
        Reset all xskipper settings by:

        1. disables filtering
        2. clear all MetadataHandle-s in the default MetadataStoreManager
        3. reset the JVM wide configuration

        :param sparkSession: SparkSession object
        """
        sparkSession._jvm.io.xskipper.Xskipper.reset(sparkSession._jsparkSession)

    @staticmethod
    def listIndexes(sparkSession):
        """
        Returns information about the indexed datasets

        :param sparkSession: SparkSession object
        :return: dataFrame object containing information about the indexed datasets
                 under the configured base path
        """
        return DataFrame(sparkSession._jvm.io.xskipper.Xskipper.listIndexes(sparkSession._jsparkSession),
                         sparkSession._wrapped)

    @staticmethod
    def enable(sparkSession):
        """
        Enable xskipper by adding the necessary rules

        :param sparkSession: SparkSession object
        """
        sparkSession._jvm.io.xskipper.Xskipper.enable(sparkSession._jsparkSession)

    @staticmethod
    def disable(sparkSession):
        """
        Disable xskipper by disabling the rules

        :param sparkSession: SparkSession object
        """
        sparkSession._jvm.io.xskipper.Xskipper.disable(sparkSession._jsparkSession)

    @staticmethod
    def isEnabled(sparkSession):
        """
        Checks whether xskipper is enabled

        :param sparkSession: SparkSession object
        :return: true if xskipper is enabled
        """
        return sparkSession._jvm.io.xskipper.Xskipper.isEnabled(sparkSession._jsparkSession)

    @staticmethod
    def installExceptionWrapper():
        """
        Install a wrapper to wrap JVM XskipperException as python XskipperException
        """
        orig = py4j.java_gateway.get_return_value
        wrapped_f = get_wrapped_function(orig)
        py4j.java_gateway.get_return_value = wrapped_f


class XskipperException(Exception):
    def __init__(self, desc, stackTrace):
        self.desc = desc
        self.stackTrace = stackTrace

    def __str__(self):
        return repr(self.desc)


def get_wrapped_function(f):
    def f_wrapped(*args, **kwargs):
        try:
            res = f(*args, **kwargs)
            return res
        except py4j.protocol.Py4JJavaError as e:
            s = e.java_exception.toString()
            stackTrace = '\n\t at '.join(map(lambda x: x.toString(), e.java_exception.getStackTrace()))
            if (s.startswith("io.xskipper")):
                raise XskipperException(s.split(': ', 1)[1] + ' ' + stackTrace, stackTrace)
            else:
                raise

    return f_wrapped
