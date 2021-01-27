# Copyright 2021 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

import unittest
import sys

from xskipper import Registration
from xskipper.testing.utils import XskipperTestCase


class XskipperRegistrationTests(XskipperTestCase):
    def setUp(self):
        super(XskipperRegistrationTests, self).setUp()

    def tearDown(self):
        super(XskipperRegistrationTests, self).tearDown()

    # tests to check the API works fine (logic is tested in Scala)
    def test_registration(self):
        # Add MetadataFilterFactor
        Registration.addMetadataFilterFactory(self.spark, 'io.xskipper.search.filters.BaseFilterFactory')
        # Add IndexFactory
        Registration.addIndexFactory(self.spark, 'io.xskipper.index.BaseIndexFactory')
        # Add MetaDataTranslator
        Registration.addMetaDataTranslator(self.spark, 'io.xskipper.metadatastore.parquet.ParquetBaseMetaDataTranslator')
        # Add ClauseTranslator
        Registration.addClauseTranslator(self.spark, 'io.xskipper.metadatastore.parquet.ParquetBaseClauseTranslator')

if __name__ == "__main__":
    xskipper_test = unittest.TestLoader().loadTestsFromTestCase(XskipperRegistrationTests)
    result = unittest.TextTestRunner(verbosity=3).run(xskipper_test)
    sys.exit(not result.wasSuccessful())
