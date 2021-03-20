/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.types

import java.nio.file.Files

import io.xskipper.implicits._
import io.xskipper.metadatastore.parquet.{ParquetMetadataStoreConf, ParquetMetadataStoreManager}
import io.xskipper.testing.util.Utils
import io.xskipper.{Xskipper, XskipperProvider}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Test suite to checks the type support for saving partition values in the metadata
  * to enable getting only relevant according to the partition filters
  * The following types are supported by Spark for partitioning -
  * numeric data types, date, timestamp and string type are supported
  */
abstract class PartitionTypeSupport (override val datasourceV2: Boolean)
  extends FunSuite with BeforeAndAfterEach with XskipperProvider with Logging {
  import spark.implicits._

  override def getXskipper(uri: String): Xskipper = {
    new Xskipper(spark, uri, ParquetMetadataStoreManager)
  }

  protected def getXskippeWithMdLocationType(uri: String,
                                             mdLocation: String,
                                             mdType: String): Xskipper = {
    val xskipper = getXskipper(uri)
    val params = Map[String, String](
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY -> mdLocation,
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY
        -> mdType)
    xskipper.setParams(params)
    xskipper
  }

  def getXskipperWithHiveTableName(uri: String, tableName: String): Xskipper = {
    getXskippeWithMdLocationType(uri, tableName,
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME)
  }

  def getXskipperWithHiveDbName(uri: String, dbName: String): Xskipper = {
    getXskippeWithMdLocationType(uri, dbName,
      ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_DB_NAME)
  }

  override def beforeEach(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    spark.sql(s"USE $databaseName")
    // set the base location for indexing
    spark.sql(
      s"ALTER DATABASE $databaseName SET DBPROPERTIES " +
        s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}'='${md_base_path}')")
  }

  override def afterEach(): Unit = {
    spark.disableXskipper()
  }

  // set base path
  val databaseName = "raymond"
  val md_base_path = Files.createTempDirectory("hms_suite_parquet").toFile
  md_base_path.deleteOnExit()

  val basePath = Utils.concatPaths(System.getProperty("user.dir"),
    "src/test/resources/input_datasets/type_support/partitioned/")

  test("Query with predicate on partition of binary type") {
    val tableName = "binarypartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "binary")

    // create table
    createTable(tableName, inputPath, "binaryType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from binarypartitionedtable where" +
      " stringType = 'abc'").where($"binaryType" > lit(Array[Byte](1.toByte, 2.toByte))).show()
  }

  test("Query with predicate on partition of boolean type") {
    val tableName = "booleanpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "boolean")

    // create table
    createTable(tableName, inputPath, "booleanType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from booleanpartitionedtable where" +
      " booleanType = true and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of byte type") {
    val tableName = "bytepartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "byte")

    // create table
    createTable(tableName, inputPath, "byteType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from bytepartitionedtable where" +
      " byteType = 1 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of date type") {
    val tableName = "datepartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "date")

    // create table
    createTable(tableName, inputPath, "dateType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from datepartitionedtable where" +
      " dateType = '2018-02-27' and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of decimal type") {
    val tableName = "decimalpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "decimal")

    // create table
    createTable(tableName, inputPath, "decimalType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from decimalpartitionedtable where" +
      " decimalType = 1 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of double type") {
    val tableName = "doublepartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "double")

    // create table
    createTable(tableName, inputPath, "doubleType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from doublepartitionedtable where" +
      " doubleType = 1.0 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of float type") {
    val tableName = "floatpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "float")

    // create table
    createTable(tableName, inputPath, "floatType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from floatpartitionedtable where" +
      " floatType = 1.0 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of integer type") {
    val tableName = "intpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "integer")

    // create table
    createTable(tableName, inputPath, "integerType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from intpartitionedtable where" +
      " integerType = 1 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of long type") {
    val tableName = "longpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "long")

    // create table
    createTable(tableName, inputPath, "longType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from longpartitionedtable where" +
      " longType = 1 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of short type") {
    val tableName = "shortpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "short")

    // create table
    createTable(tableName, inputPath, "shortType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "stringType")

    spark.sql("select * from shortpartitionedtable where" +
      " shortType = 1 and stringType = 'abc'").show()
  }

  test("Query with predicate on partition of string type") {
    val tableName = "stringpartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "string")

    // create table
    createTable(tableName, inputPath, "shortType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "shortType")

    spark.sql("select * from stringpartitionedtable where" +
      " stringType = 'abc' and shortType = 1").show()
  }

  test("Query with predicate on partition of timestamp type") {
    val tableName = "timestamppartitionedtable"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "string")

    // create table
    createTable(tableName, inputPath, "timestampType")

    // index and enable filtering
    indexAndEnableXskipper(fullTableName, "shortType")

    spark.sql("select * from timestamppartitionedtable where" +
      " timestampType = to_timestamp('2018-03-28 03:06:43') and shortType = 1").show()
  }

  test("nested partitions") {
    val tableName = "nestedpartitioning"
    val fullTableName = s"$databaseName.$tableName"
    val inputPath = Utils.concatPaths(basePath, "nested")

    // create hive metastore table
    val createTable = s"""CREATE TABLE IF NOT EXISTS ${tableName} (
            integerType Int,
            stringType String,
            byteType Byte,
            shortType Short,
            longType Long,
            floatType Float,
            doubleType Double,
            decimalType Decimal(10,0),
            binaryType Binary,
            booleanType Boolean,
            timestampType Timestamp,
            dateType Date
          )
          USING PARQUET
          PARTITIONED BY (dateType, stringType)
          LOCATION '${inputPath}'"""

    spark.sql(createTable)

    // index the dataset - using the table name
    val xskipper = getXskipperWithHiveDbName(fullTableName, databaseName)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add some index
    xskipper.indexBuilder()
      .addValueListIndex("longType")
      .build()

    spark.sql("select * from nestedpartitioning where" +
      " stringType = 'abc' and longType = 1").show()

    spark.sql("select * from nestedpartitioning where" +
      " stringType = 'abc' and dateType = '2018-02-27' and longType = 1").show()
  }

  private def indexAndEnableXskipper(fullTableName: String, indexColumn: String): Unit = {
    // index the dataset - using the table name
    val xskipper = getXskipperWithHiveDbName(fullTableName, databaseName)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    // add some index
    xskipper.indexBuilder()
      .addValueListIndex(indexColumn)
      .build()

    // run query with a predicate on the int column
    spark.enableXskipper()
  }

  private def createTable(tableName: String, tableLocation: String,
                          partitionColumn: String): Unit = {
    // drop table if it already exists
    spark.sql(s"drop table if exists $tableName")

    // create hive metastore table
    val createTable = s"""CREATE TABLE IF NOT EXISTS $tableName (
            integerType Int,
            stringType String,
            byteType Byte,
            shortType Short,
            longType Long,
            floatType Float,
            doubleType Double,
            decimalType Decimal(10,0),
            binaryType Binary,
            booleanType Boolean,
            timestampType Timestamp,
            dateType Date
          )
          USING PARQUET
          PARTITIONED BY ($partitionColumn)
          LOCATION '${tableLocation}'"""

    spark.sql(createTable)

    // recover partitions
    spark.sql(s"ALTER TABLE $tableName RECOVER PARTITIONS")
  }
}
