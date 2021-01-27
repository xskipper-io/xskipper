/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.spark.sql.types.xskipper.utils

import io.xskipper.metadatastore.parquet.UpgradeDescriptor
import org.apache.spark.sql.DataFrame

object DataFrameColumnUpgrader {

  def applyUpgradeDescriptor(df: DataFrame, upgradeDescriptor: UpgradeDescriptor): DataFrame = {
    if (upgradeDescriptor.origColName != upgradeDescriptor.newColName) {
      // the new col name is not the same as the old one - no need to use a middle column
      df.withColumn(upgradeDescriptor.newColName,
        upgradeDescriptor.upgradeColumn,
        upgradeDescriptor.newMetadata)
        .drop(upgradeDescriptor.origColName)
    } else {
      // the new name is the same as the old one, use a middle column
      val middleColName = "foo"
      require(!df.schema.fieldNames.contains(middleColName))
      df.withColumn(middleColName,
        upgradeDescriptor.upgradeColumn,
        upgradeDescriptor.newMetadata)
        .drop(upgradeDescriptor.origColName)
        .withColumnRenamed(middleColName, upgradeDescriptor.newColName)
    }
  }
}
