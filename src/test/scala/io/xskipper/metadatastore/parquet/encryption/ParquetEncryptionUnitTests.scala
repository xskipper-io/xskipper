/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.encryption

import io.xskipper.Registration
import io.xskipper.index.{Index, MinMaxIndex}
import io.xskipper.metadatastore.parquet.ParquetUtils._
import io.xskipper.metadatastore.parquet._
import io.xskipper.metadatastore.parquet.encryption.Values._
import io.xskipper.testing.util.Utils
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.FunSuite


/**
  * Unit Tests For encrypted metadata
  */
class ParquetEncryptionUnitTests extends FunSuite {
  private lazy implicit val vanillaConverter = new SparkToParquetSchemaConverter(new SQLConf())
  Registration.setActiveMetadataStoreManager(Parquet)


  val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  val schemaExtractionBaseDir = Utils.concatPaths(System.getProperty("user.dir"),
    "src/test/resources/parquet_version_tests/schema_extraction")

  val currVersion = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION
  // make sure this suite runs in V4 - update it if bumping the version!
  assert(currVersion == 4L)


  /**
    * The "story" behind the test is as follows:
    * there exist keys k1,k2,k3 and someone ran this code:
    * {{{
    *   im.indexBuilder
    *     .addMinMaxIndex("temp", "k1")
    *     .addBloomFilterIndex("city", "k1")
    *     .addValueListIndex("city")
    *     .build(spark.read.format("parquet"))
    * }}}
    */

  def genIdxMD(index: Index, tid: String = tableIdentifier): Metadata = {
    // build one metadata to hold all index metadata
    val indexMeta = new sql.types.MetadataBuilder()
      .putString("name", index.getName)
      .putStringArray("cols", index.getCols.toArray)
    // adding params if necessary
    if (index.getParams.nonEmpty) {
      val params = new sql.types.MetadataBuilder()
      index.getParams.foreach {
        case (key: String, value: String) => params.putString(key, value)
      }
      indexMeta.putMetadata("params", params.build())
    }
    // Add Key Metadata if defined
    if (index.isEncrypted()) {
      indexMeta.putString("key_metadata", index.getKeyMetadata().get)
    }

    // add the metadata under the index key
    new sql.types.MetadataBuilder()
      .putMetadata("index", indexMeta.build())
      .build()
  }

  def genField(idx: Index): StructField = {
    val schemaTranslators = Registration.getCurrentMetaDataTranslators().collect {
      case t: ParquetMetaDataTranslator => t
    }
    StructField(
      getColumnName(idx),
      ParquetUtils.getIndexSchema(idx, schemaTranslators).getOrElse(
        ParquetMetadataStoreUDTRegistrator.getUDTFor(idx.getMetaDataTypeClassName())),
      true,
      genIdxMD(idx))
  }

  val minMaxMeta = genIdxMD(minMaxIdx)
  val bloomFilterMeta = genIdxMD(bloomFilterIdx)
  val valListMeta = genIdxMD(valListIdx)

  test("Test DataFrameWriter Options for encrypted metadata with encrypted footer") {
    val schema = StructType(Seq(
      StructField("obj_name", StringType, false, masterMetaEncryptedFooter)))
    val expectedRes = scala.collection.immutable.HashMap[String, String](
      (ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SHORT_KEY, columnKeyListStringEncryptedFooter),
      (ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_PME_KEY, columnKeyListStringEncryptedFooter),
      (ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SHORT_KEY, footerKeyLabel),
      (ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_PME_KEY, footerKeyLabel)

    )

    val actualRes = getDFWriterOptions(schema)

    assertResult(expectedRes)(actualRes)
  }

  test("Test DataFrameWriter Options for encrypted metadata with plaintext footer") {
    val schema = StructType(Seq(
      StructField("obj_name", StringType, false, masterMetaPlaintextFooter)))
    val expectedRes = scala.collection.immutable.HashMap[String, String](
      (ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SHORT_KEY, columnKeyListStringPlaintextFooter),
      (ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_PME_KEY, columnKeyListStringPlaintextFooter),
      (ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_SHORT_KEY, "true"),
      (ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_PME_KEY, "true"),
      (ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SHORT_KEY, footerKeyLabel),
      (ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_PME_KEY, footerKeyLabel)
    )

    val actualRes = getDFWriterOptions(schema)

    assertResult(expectedRes)(actualRes)
  }

  test("Test Generation of the Column Keys string with encrypted footer") {
    assertResult(columnKeyListStringEncryptedFooter)(
      getColumnKeyListString(indexes, Some(partitionSchema), footerKeyLabel))
  }


  test("Test Generation of the master metadata with encrypted footer") {
    assertResult(masterMetaEncryptedFooter)(
      createMasterMetadata(indexes, Some(partitionSchema),
        tableIdentifier, Some(footerKeyLabel), false))
  }


  test("Test Generation of the master metadata with plaintext footer") {
    assertResult(masterMetaPlaintextFooter)(
      createMasterMetadata(indexes, Some(partitionSchema),
        tableIdentifier, Some(footerKeyLabel), true))
  }

  test("Test Generation of the master metadata with encrypted footer," +
    " verify exception thrown when no footer key supplied") {
    assertThrows[ParquetMetaDataStoreException] {
      createMasterMetadata(indexes, Some(partitionSchema), tableIdentifier, None, false)
    }
  }

  test("Test Generation of the master metadata with plaintext footer," +
    " verify exception thrown when no footer key supplied") {
    assertThrows[ParquetMetaDataStoreException] {
      createMasterMetadata(indexes, Some(partitionSchema), tableIdentifier, None, true)
    }
  }

  test("Test Generation of the master metadata," +
    " verify exception thrown when footer key supplied but no indexes are encrypted") {
    assertThrows[ParquetMetaDataStoreException] {
      createMasterMetadata(Seq(MinMaxIndex("temp")), Some(partitionSchema),
        tableIdentifier, Some(footerKeyLabel), false)
    }
  }

  test("Test Generation of the per-index metadata") {
    assertResult(minMaxMeta)(createIndexMetadata(minMaxIdx))
    assertResult(bloomFilterMeta)(createIndexMetadata(bloomFilterIdx))
    assertResult(valListMeta)(createIndexMetadata(valListIdx))
  }

  test("Test Schema Creation with encrypted footer ") {
    val objNameField = StructField("obj_name",
      StringType, false, masterMetaEncryptedFooter)

    val expectedSchema = StructType(
      Seq(objNameField, expectedDtField, expectedYearField) ++ indexes.map(genField)
    )

    val actualResult = createDFSchema(indexes,
      Some(partitionSchema),
      true,
      tableIdentifier,
      Some(footerKeyLabel),
      false)

    assertResult(expectedSchema)(actualResult)
  }


  test("Test Schema Creation with Plaintext footer") {
    val objNameField = StructField("obj_name",
      StringType, false, masterMetaPlaintextFooter)

    val expectedSchema = StructType(
      Seq(objNameField, expectedDtField, expectedYearField) ++ indexes.map(genField)
    )

    val actualResult = createDFSchema(indexes,
      Some(partitionSchema),
      true,
      tableIdentifier,
      Some(footerKeyLabel),
      true)

    assertResult(expectedSchema)(actualResult)
  }

  def testSchemaExtractionPlaintextFooter(version: Long): Unit = {
    test(s"Test schema extraction, V${version} plaintext footer") {
      val objNameField = StructField("obj_name",
        StringType, nullable = false, masterMetaPlaintextFooter)

      val expectedSchema = StructType(
        Seq(objNameField, expectedDtField, expectedYearField) ++ indexes.map(genField)
      )

      val inputLocation = Utils.concatPaths(schemaExtractionBaseDir,
        s"version=${version}/plaintext_footer/schema.json")
      val inputSchemaString = Utils.readTextFileAsString(inputLocation)
      val inputSchema = DataType.fromJson(inputSchemaString).asInstanceOf[StructType]
      // the version on the schema should be the same as the input version
      assertResult(version)(ParquetUtils.getVersion(inputSchema))

      // make sure the schema is extracted correctly
      val actualSchema = extractSchema(inputSchema, indexes, Some(partitionSchema))
      assertResult(expectedSchema)(actualSchema)
    }
  }

  def testSchemaExtractionEncryptedFooter(version: Long): Unit = {
    test(s"Test schema extraction, V${version} encrypted footer") {

      val objNameField = StructField("obj_name",
        StringType, nullable = false, masterMetaEncryptedFooter)

      val expectedSchema = StructType(
        Seq(objNameField, expectedDtField, expectedYearField) ++ indexes.map(genField)
      )

      val inputLocation = Utils.concatPaths(schemaExtractionBaseDir,
        s"version=${version}/encrypted_footer/schema.json")
      val inputSchemaString = Utils.readTextFileAsString(inputLocation)
      val inputSchema = DataType.fromJson(inputSchemaString).asInstanceOf[StructType]

      // the version on the schema should be the same as the input version
      assertResult(version)(ParquetUtils.getVersion(inputSchema))
      // make sure the schema is extracted correctly
      val actualSchema = extractSchema(inputSchema, indexes, Some(partitionSchema))
      assertResult(expectedSchema)(actualSchema)
    }
  }

  // test schema extraction
  (1L to currVersion).foreach(v => {
    testSchemaExtractionPlaintextFooter(v)
    testSchemaExtractionEncryptedFooter(v)
  })

}
