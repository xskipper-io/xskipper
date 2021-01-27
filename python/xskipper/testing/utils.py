#
# Copyright (2020) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This file contains code from the Apache Spark project (original license above)
# and Delta Lake project (same Apache 2.0 license above).
# It contains modifications, which are licensed as follows:
#

# Copyright 2021 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

import unittest
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class XskipperTestCase(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.spark = SparkSession.builder \
            .master("local[4]") \
            .config("spark.ui.enabled", "false") \
            .appName(class_name) \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()
        sys.path = self._old_sys_path