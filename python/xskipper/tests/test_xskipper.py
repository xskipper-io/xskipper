# Copyright 2021 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

import unittest
import sys
import tempfile
import shutil

from pyspark.sql.types import *

from xskipper import Xskipper
from xskipper import Registration
from xskipper.testing.utils import XskipperTestCase


class XskipperTests(XskipperTestCase):
    def setUp(self):
        self.dataset_location = tempfile.mkdtemp()
        self.metadata_location = tempfile.mkdtemp()
        super(XskipperTests, self).setUp()

    def tearDown(self):
        shutil.rmtree(self.dataset_location)
        shutil.rmtree(self.metadata_location)
        super(XskipperTests, self).tearDown()

    def test_xskipper_enable_disable(self):
        self.assertFalse(Xskipper.isEnabled(self.spark),
                         "Xskipper should be disabled by default.")
        Xskipper.enable(self.spark)
        self.assertTrue(Xskipper.isEnabled(self.spark),
                        "Xskipper should be enabled after calling enableXskipper.")
        Xskipper.disable(self.spark)
        self.assertFalse(Xskipper.isEnabled(self.spark),
                         "Xskipper should be disabled after calling disableXskipper.")

    def test_xskipper_configuration(self):
        # Set the active MetadataStoreManager
        Registration.setActiveMetadataStoreManager(self.spark,
                               'io.xskipper.metadatastore.parquet.Parquet')
        self.assertEqual(Registration.getActiveMetadataStoreManagerType(self.spark),
                          'io.xskipper.metadatastore.parquet.Parquet$')
        # Setting JVM wide xskipper parameters
        conf = dict([
            ('io.xskipper.parquet.mdlocation', self.metadata_location),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        Xskipper.setConf(self.spark, conf)
        # check the values were set
        self.assertEqual(Xskipper.get(self.spark, 'io.xskipper.parquet.mdlocation'),
                          self.metadata_location)
        self.assertEqual(Xskipper.get(self.spark, 'io.xskipper.parquet.mdlocation.type'),
                          "EXPLICIT_BASE_PATH_LOCATION")
        # check getting all conf
        print(Xskipper.getConf(self.spark))
        # check setting value individually
        Xskipper.set(self.spark, 'io.xskipper.index.bloom.fpp', '0.2')
        self.assertEqual(Xskipper.get(self.spark, 'io.xskipper.index.bloom.fpp'), "0.2")
        # unset a configuration
        Xskipper.unset(self.spark, 'io.xskipper.index.bloom.fpp')
        self.assertEqual(Xskipper.get(self.spark, 'io.xskipper.index.bloom.fpp'), None)

    # making sure all methods can be called (the logic is tested in the scala tests)
    def test_xskipper_methods(self):
        # getting latest stats:
        Xskipper.getLatestQueryAggregatedStats(self.spark).show()
        # clearing stats
        Xskipper.clearStats(self.spark)
        # reset
        Xskipper.reset(self.spark)
        # list indexes - use /tmp/metadata as a dummy basepath location
        conf = dict([
            ('io.xskipper.parquet.mdlocation', self.metadata_location),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        Xskipper.setConf(self.spark, conf)
        Xskipper.listIndexes(self.spark).show(10, False)
        # install exception wrapper
        Xskipper.installExceptionWrapper()

    def test_indexing_dataset(self):
        Xskipper.installExceptionWrapper()
        conf = dict([
            ('io.xskipper.parquet.mdlocation', self.metadata_location),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        Xskipper.setConf(self.spark, conf)
        # create sample dataset for test
        schema = StructType([StructField("dt", StringType(), True), StructField("temp", DoubleType(), True), \
                       StructField("city", StringType(), True), StructField("vid", StringType(), True)])
        data = [("2017-07-07", 20.0, "Tel-Aviv", "a"), ("2017-07-08", 30.0, "Jerusalem", "b")]
        df = self.spark.createDataFrame(data, schema=schema)
        # use partitionBy to make sure we have two objects
        df.write.partitionBy("dt").mode("overwrite").parquet(self.dataset_location)

        # indexing
        reader = self.spark.read.format("parquet")
        xskipper = Xskipper(self.spark, self.dataset_location)

        # call set params to make sure it overwrites JVM wide config
        params = dict([
            ('io.xskipper.parquet.mdlocation', "{0}_parquet".format(self.metadata_location)),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        xskipper.setParams(params)

        # remove index if exists
        if xskipper.isIndexed():
            xskipper.dropIndex()

        # test adding all index types including using the custom index API
        xskipper.indexBuilder() \
            .addMinMaxIndex("temp") \
            .addValueListIndex("city") \
            .addBloomFilterIndex("vid") \
            .addCustomIndex('io.xskipper.index.MinMaxIndex', ['city'], dict()) \
            .build(reader) \
            .show(10, False)

        # validate dataset is indexed
        self.assertTrue(xskipper.isIndexed(), 'Dataset should be indexed following index building')

        # describe the index
        xskipper.describeIndex(reader).show(10, False)

        # refresh the index
        xskipper.refreshIndex(reader).show(10, False)

        # reset after indexing to make sure we don't use the cached MetadataHandle
        Xskipper.reset(self.spark)
        conf = dict([
            ('io.xskipper.parquet.mdlocation', "{0}_parquet".format(self.metadata_location)),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        Xskipper.setConf(self.spark, conf)

        # list the indexed datasets
        Xskipper.listIndexes(self.spark).show(10, False)

        # enable skipping and run test query
        Xskipper.enable(self.spark)
        df = reader.load(self.dataset_location)
        df.createOrReplaceTempView("metergen")

        self.spark.sql("select count(*) from metergen where temp < 30").show()

        # get latest stats
        # JVM wide
        Xskipper.getLatestQueryAggregatedStats(self.spark).show(10, False)
        # get specific stats for the xskipper instance
        xskipper.getLatestQueryStats().show(10, False)

        # drop the index and verified it is dropped
        xskipper.dropIndex()
        self.assertFalse(xskipper.isIndexed(), 'Dataset should not be indexed following index dropping')
        # clearing stats
        Xskipper.clearStats(self.spark)

    def test_indexing_hive(self):
        Xskipper.installExceptionWrapper()
        conf = dict([
            ('io.xskipper.parquet.mdlocation', "{0}".format(self.metadata_location)),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        Xskipper.setConf(self.spark, conf)
        # create sample dataset for test
        schema = StructType([StructField("dt", StringType(), True), StructField("temp", DoubleType(), True), \
                             StructField("city", StringType(), True), StructField("vid", StringType(), True)])
        data = [("2017-07-07", 20.0, "Tel-Aviv", "a"), ("2017-07-08", 30.0, "Jerusalem", "b")]
        df = self.spark.createDataFrame(data, schema=schema)
        # use partitionBy to make sure we have two objects
        df.write.partitionBy("dt").mode("overwrite").parquet(self.dataset_location)

        # set the base location in the database
        alter_db_ddl = ("ALTER DATABASE default SET DBPROPERTIES ('io.xskipper.parquet.mdlocation'='{0}')").format(self.metadata_location)
        self.spark.sql(alter_db_ddl)

        # create the table
        create_table_ddl = """CREATE TABLE IF NOT EXISTS tbl ( \
        temp Double,
        city String,
        vid String,
        dt String
        )
        USING PARQUET
        PARTITIONED BY (dt)
        LOCATION '{0}'""".format(self.dataset_location)
        self.spark.sql(create_table_ddl)

        # recover partitions
        self.spark.sql("ALTER TABLE tbl RECOVER PARTITIONS")

        # verify the table was created
        self.spark.sql("show tables").show(10, False)

        self.spark.sql("show partitions tbl").show(10, False)

        # indexing
        xskipper = Xskipper(self.spark, 'default.tbl')

        # remove index if exists
        if xskipper.isIndexed():
            xskipper.dropIndex()

        # test adding all index types including using the custom index API
        xskipper.indexBuilder() \
            .addMinMaxIndex("temp") \
            .addValueListIndex("city") \
            .addBloomFilterIndex("vid") \
            .addCustomIndex('io.xskipper.index.MinMaxIndex', ['city'], dict()) \
            .build() \
            .show(10, False)

        # validate dataset is indexed
        self.assertTrue(xskipper.isIndexed(), 'Hive table should be indexed following index building')

        # describe the index
        xskipper.describeIndex().show(10, False)

        # refresh the index
        xskipper.refreshIndex().show(10, False)

        # reset after indexing to make sure we don't use the cached MetadataHandle
        Xskipper.reset(self.spark)
        conf = dict([
            ('io.xskipper.parquet.mdlocation', "{0}".format(self.metadata_location)),
            ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
        Xskipper.setConf(self.spark, conf)

        # list the indexed datasets
        Xskipper.listIndexes(self.spark).show(10, False)

        # enable skipping and run test query
        Xskipper.enable(self.spark)

        self.spark.sql("select count(*) from tbl where temp < 30").show()

        # get latest stats
        # JVM wide
        Xskipper.getLatestQueryAggregatedStats(self.spark).show(10, False)
        # get specific stats for the xskipper instance
        xskipper.getLatestQueryStats().show(10, False)

        # drop the index and verified it is dropped
        xskipper.dropIndex()
        self.assertFalse(xskipper.isIndexed(), 'Hive table should not be indexed following index dropping')
        # clearing stats
        Xskipper.clearStats(self.spark)

if __name__ == "__main__":
    xskipper_test = unittest.TestLoader().loadTestsFromTestCase(XskipperTests)
    result = unittest.TextTestRunner(verbosity=3).run(xskipper_test)
    sys.exit(not result.wasSuccessful())
