/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.encryption.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import io.xskipper.metadatastore.parquet.ParquetMetadataStoreConf
import io.xskipper.metadatastore.parquet.ParquetUtils.createDFSchema
import io.xskipper.metadatastore.parquet.encryption.Values.{footerKeyLabel, indexes, partitionSchema, tableIdentifier}
import io.xskipper.testing.util.Utils
import org.apache.spark.sql.SparkSession

object SchemaGenerator {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val schemaExtractionBaseDir = Utils.concatPaths(System.getProperty("user.dir"),
    "src/test/resources/parquet_version_tests/schema_extraction")
  val currVersion = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION

  def main(args: Array[String]): Unit = {
    writeSchemaToFile(schemaExtractionBaseDir, false)
    writeSchemaToFile(schemaExtractionBaseDir, true)
  }

  def writeSchemaToFile(baseDir: String, plaintextFooter: Boolean): Unit = {

    val writeLoc = plaintextFooter match {
      case true => Utils.concatPaths(baseDir,
        s"version=${currVersion}/plaintext_footer")
      case false => Utils.concatPaths(baseDir,
        s"version=${currVersion}/encrypted_footer")
    }

    val schema = createDFSchema(indexes,
      Some(partitionSchema),
      true,
      tableIdentifier,
      Some(footerKeyLabel),
      plaintextFooter)

    Paths.get(writeLoc).toFile.mkdirs()
    Files.write(Paths.get(Utils.concatPaths(writeLoc, "schema.json")),
      schema.json.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
  }

}
