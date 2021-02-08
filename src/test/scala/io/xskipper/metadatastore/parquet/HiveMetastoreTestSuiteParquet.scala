/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import java.nio.file.Files

import io.xskipper._
import io.xskipper.implicits._
import io.xskipper.testing.util.Utils._
import io.xskipper.testing.util.{LogTrackerBuilder, Utils}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  *
  * Test suite to check data skipping integration with hive metastore
  * Note: tests that check for skipping files need the logging for
  * io.xskipper.search to be on DEBUG level
  * This can also be set by having log4j-defaults.properties
  * in the resources folder and setting there:
  * log4j.logger.io.xskipper.search=DEBUG
  */

abstract class HiveMetastoreTestSuiteParquet(override val datasourceV2: Boolean = false)
  extends FunSuite with XskipperProvider with BeforeAndAfterEach {


  override def beforeEach(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    spark.sql(s"USE $databaseName")
  }

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

  // monitor skipped files
  val regexp = "(.*).*#.*--------> SKIPPED!".r
  val skippedFiles = LogTrackerBuilder.getRegexTracker(regexp)

  val baseDir = Utils.concatPaths(System.getProperty("user.dir"), "src/test/resources")

  val databaseName = "raymond"
  val tableName = "metergen"
  val fullTableName = s"$databaseName.$tableName"
  val viewName = s"${tableName}_view"
  val fullViewName = s"$databaseName.$viewName"
  // set debug log level specifically for xskipper package
  LogManager.getLogger("io.xskipper").setLevel(Level.DEBUG)


  def testHiveTable(useLegacyLocationInDatabase: Boolean,
                    useLegacyLocationInTable: Boolean): Unit = {
    test(testName = "test Hive table with explicit base" +
      s" location set in the database," +
      s" legacy location in database: $useLegacyLocationInDatabase," +
      s"legacy location in table: $useLegacyLocationInTable") {
      val input_path = Utils.concatPaths(baseDir, "input_datasets/gridpocket/initial/parquet/")
      val md_base_path = Files.createTempDirectory("hms_suite_parquet").toString
      md_base_path.deleteOnExit()

      // set the base metadata location in the database
      val locParam = useLegacyLocationInDatabase match {
        case false => ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY
        case true => ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY
      }

      spark.sql(
        s"ALTER DATABASE $databaseName SET DBPROPERTIES " +
          s"('$locParam'='${md_base_path}')")

      // drop table if it already exists
      spark.sql(s"drop table if exists $fullTableName")
      // create hive metastore table
      val createTable =
        s"""CREATE TABLE IF NOT EXISTS $fullTableName (
            vid String,
            date Timestamp,
            index Double,
            sumHC Double,
            sumHP Double,
            type String,
            size Integer,
            temp Double,
            city String,
            region Integer,
            lat Double,
            lng Double,
            dt Date
          )
          USING PARQUET
          PARTITIONED BY (dt)
          LOCATION '${input_path}'"""
      spark.sql(createTable)

      // add the relevant partitions under the base path
      spark.sql(s"ALTER TABLE $fullTableName RECOVER PARTITIONS")
      // equivalent to:
      // spark.sql(s"ALTER TABLE metergen add partition " +
      //  s"(dt='2017-07-15') LOCATION '$input_path/dt=2017-07-15'")
      // spark.sql(s"ALTER TABLE metergen add partition " +
      //  s"(dt='2017-07-16') LOCATION '$input_path/dt=2017-07-16'")

      // add one more partition under specific location to verify that the when
      // indexing this partition is read as well
      val sidePartitionLocation = Utils.concatPaths(baseDir,
        "input_datasets/gridpocket/updated/parquet/dt=2017-07-08")
      spark.sql(
        s"ALTER TABLE $fullTableName ADD" +
          s" PARTITION(dt='2017-07-08') location '$sidePartitionLocation'")

      // index the dataset - using the table name
      // During indexing the path in the table is not yet defined so there will be a fall back to
      // db base location (and if it doesn't exist to the JVM base location)
      val xskipper = getXskipperWithHiveTableName(fullTableName, fullTableName)

      // remove existing index first
      if (xskipper.isIndexed()) {
        xskipper.dropIndex()
      }
      val indexBuildRes = xskipper.indexBuilder()
        .addMinMaxIndex("temp")
        .addValueListIndex("city")
        .build()
      assert(Utils.isResDfValid(indexBuildRes))

      // enable filtering
      spark.enableXskipper()

      val tableIdentifier = TableIdentifier(tableName, Some(databaseName))

      def assertTableProperties(): Unit = {
        // assert the table contains the correct metadata parameter
        val tableProperties = spark.sessionState.
          catalog.getTableMetadata(tableIdentifier).properties
        assert(tableProperties.contains(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY),
          "Table metadata does not contain the xskipper location property after indexing")
        assert(!tableProperties.contains(ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY),
          "Table metadata contains the legacy metadata location property!")
      }

      assertTableProperties()
      // set the database property to a fake dummy value- this is to ensure
      // no fallback happens here and that the metadata is resolved from the table
      spark.sql(
        s"ALTER DATABASE $databaseName SET DBPROPERTIES " +
          s"('$locParam'='/foo/bar')")
      // if set, place the metadata location in the old location property and
      // remove the current one to test correct handling
      if (useLegacyLocationInTable) {
        val actualMdPath = spark.sessionState.catalog.getTableMetadata(tableIdentifier)
          .properties(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY)
        spark.sql(
          s"ALTER TABLE ${fullTableName} SET TBLPROPERTIES " +
            s"('${ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY}'='${actualMdPath}')")
        spark.sql(
          s"ALTER TABLE ${fullTableName} UNSET TBLPROPERTIES IF EXISTS " +
            s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}')")
        // refresh the table metadata
        spark.catalog.refreshTable(fullTableName)
      }

      // set the expected skipped files
      val expectedSkippedFiles = Seq(
        "dt=2017-07-16/c7.snappy.parquet",
        "dt=2017-07-16/c8.snappy.parquet",
        "dt=2017-07-15/c1.snappy.parquet",
        "dt=2017-07-15/c0.snappy.parquet")
      // include the main table location and the side partition
      val expectedSkippedFilesFormatted =
        Utils.getResultSet(input_path, expectedSkippedFiles: _*) ++
          Set(Utils.concatPaths(sidePartitionLocation, "c0.snappy.parquet"))


      // execute query and monitor skipped files
      skippedFiles.clearSet()
      skippedFiles.startCollecting()

      spark.sql(s"select * from $tableName where temp > 30").collect()

      skippedFiles.stopCollecting()

      // assert skipping was successful
      assertResult(expectedSkippedFilesFormatted)(skippedFiles.getResultSet())

      // Test query stats
      // note we don't need to factor the number
      // of skipped files as this will always run as v1
      assertResult(Utils.getNumSkippedFilesAndClear())(skippedFiles.getResultSet().size)

      // verify that after refresh only the correct HMS params are set in the table
      val fileToTouch = expectedSkippedFilesFormatted.head
      FileUtils.touch(fileToTouch)
      xskipper.refreshIndex()
      assert(Utils.isResDfValid(xskipper.refreshIndex()))
      assertTableProperties()

      // drop the index and make sure the property doesn't exist anymore.
      xskipper.dropIndex()
      assert(!spark.sessionState.catalog
        .getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
        .properties.contains(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY),
        "Table metadata still contains the xskipper location property after dropping index")
      // assert the table contains the correct metadata parameter
      val updatedTableProperties = spark.sessionState
        .catalog.getTableMetadata(tableIdentifier).properties
      assert(!updatedTableProperties.contains(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY),
        "Table metadata still contains the xskipper location property")
      assert(!updatedTableProperties.contains(
        ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY),
        "Table metadata still contains the legacy metadata location property")

      // drop the created table
      spark.sql(s"drop table if exists $fullTableName")
    }
  }

  testHiveTable(false, false)
  testHiveTable(false, true)
  testHiveTable(true, false)
  testHiveTable(true, true)

  def testDirectIndexing(useLegacyLocation: Boolean): Unit = {
    test(testName = "data skipping on non partitioned table by" +
      s" indexing the table location, legacy location = $useLegacyLocation") {
      val input_path = Utils.concatPaths(baseDir,
        "input_datasets/gridpocket/initial/parquet/dt=2017-07-16")
      val md_base_path = Files.createTempDirectory("hms_suite_parquet").toString
      md_base_path.deleteOnExit()
      // set the base metadata location in the database
      val locationParam = useLegacyLocation match {
        case false => ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY
        case true => ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY
      }

      spark.sql(
        s"ALTER DATABASE $databaseName SET DBPROPERTIES " +
          s"('${locationParam}'='${md_base_path}')")

      // drop table if it already exists
      spark.sql(s"drop table if exists $fullTableName")
      // create hive metastore table
      val createTable =
        s"""CREATE TABLE IF NOT EXISTS $fullTableName
          USING PARQUET
          LOCATION '${input_path}'
          OPTIONS(mergeSchema='true')"""
      spark.sql(createTable)

      // index the dataset - using the table name
      val xskipper = getXskipperWithHiveDbName(input_path, databaseName)

      // remove existing index first
      if (xskipper.isIndexed()) {
        xskipper.dropIndex()
      }
      val indexBuildRes = xskipper.indexBuilder()
        .addMinMaxIndex("temp")
        .build(spark.read.format("parquet"))
      assert(Utils.isResDfValid(indexBuildRes))


      // enable filtering
      spark.enableXskipper()

      // set the expected skipped files
      val expectedSkippedFilesFormatted = Utils.getResultSet(input_path,
        "c7.snappy.parquet",
        "c8.snappy.parquet")

      // execute query and monitor skipped files
      skippedFiles.clearSet()
      skippedFiles.startCollecting()

      spark.sql(s"select * from $tableName where temp > 30").collect()

      skippedFiles.stopCollecting()

      // assert skipping was successful
      assertResult(expectedSkippedFilesFormatted)(skippedFiles.getResultSet())

      // Test query stats
      // note we don't need to factor the number
      // of skipped files as this will always run as v1
      assertResult(Utils.getNumSkippedFilesAndClear())(skippedFiles.getResultSet().size)


      // drop the created table
      spark.sql(s"drop table if exists $tableName")
    }
  }

  testDirectIndexing(false)
  testDirectIndexing(true)


  test("test failure on indexing partition columns") {
    val input_path = Utils.concatPaths(baseDir, "input_datasets/gridpocket/initial/parquet/")
    val md_base_path = Files.createTempDirectory("hms_suite_parquet").toString
    md_base_path.deleteOnExit()

    // set the base metadata location in the database
    spark.sql(
      s"ALTER DATABASE $databaseName SET DBPROPERTIES " +
        s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}'='${md_base_path}')")

    // drop table if it already exists
    spark.sql(s"drop table if exists $fullTableName")
    // create hive metastore table
    val createTable =
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS $fullTableName (
            vid String,
            date Timestamp,
            index Double,
            sumHC Double,
            sumHP Double,
            type String,
            size Integer,
            temp Double,
            city String,
            region Integer,
            lat Double,
            lng Double
          )
          PARTITIONED BY (dt Date)
          STORED AS PARQUET
          LOCATION '${input_path}'
          TBLPROPERTIES ('parquet.compression'='SNAPPY' , 'transactional'='false')"""
    spark.sql(createTable)

    // add the relevant partitions under the base path
    spark.sql(s"ALTER TABLE $fullTableName RECOVER PARTITIONS")

    // add one more partition under specific location to verify that the when
    // indexing this partition is read as well
    val sidePartitionLocation = Utils.concatPaths(baseDir,
      "input_datasets/gridpocket/updated/parquet/dt=2017-07-08")
    spark.sql(
      s"ALTER TABLE $tableName ADD PARTITION(dt='2017-07-08') location '$sidePartitionLocation'")

    // index the dataset - using the table name
    val xskipper = getXskipperWithHiveTableName(fullTableName, fullTableName)

    // remove existing index first
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }
    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addValueListIndex("dt")
        .addMinMaxIndex("temp")
        .addValueListIndex("city")
        .build()
    }
  }

  test("failIndexView") {
    val input_path = Utils.concatPaths(baseDir, "input_datasets/gridpocket/initial/parquet/")
    val md_base_path = Files.createTempDirectory("hms_suite_parquet").toString
    md_base_path.deleteOnExit()

    // set the base metadata location in the database
    spark.sql(
      s"ALTER DATABASE $databaseName SET DBPROPERTIES " +
        s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}'='${md_base_path}')")

    // drop table and view if it already exists
    spark.sql(s"drop table if exists $tableName")
    spark.sql(s"drop view if exists $viewName")

    // create hive metastore table
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
            vid String,
            date Timestamp,
            index Double,
            sumHC Double,
            sumHP Double,
            type String,
            size Integer,
            temp Double,
            city String,
            region Integer,
            lat Double,
            lng Double,
            dt Date
          )
          USING PARQUET
          PARTITIONED BY (dt)
          LOCATION '${input_path}'"""
    spark.sql(createTable)

    // add the relevant partitions under the base path
    spark.sql(s"ALTER TABLE $tableName RECOVER PARTITIONS")

    // add one more partition under specific location to verify that the when
    // indexing this partition is read as well
    val sidePartitionLocation = Utils.concatPaths(baseDir,
      "input_datasets/gridpocket/updated/parquet/dt=2017-07-08")
    spark.sql(
      s"ALTER TABLE $tableName ADD PARTITION(dt='2017-07-08') location '$sidePartitionLocation'")

    // create a view
    val createView =
      s"""CREATE VIEW $viewName AS
            SELECT * from $tableName"""
    spark.sql(createView)

    // index the dataset - using the table name
    val xskipper = getXskipperWithHiveTableName(fullViewName, fullViewName)

    assertThrows[XskipperException] {
      xskipper.indexBuilder()
        .addValueListIndex("dt")
        .addMinMaxIndex("temp")
        .addValueListIndex("city")
        .build()
    }
  }
}

class HiveMetastoreTestSuiteParquetV1 extends HiveMetastoreTestSuiteParquet(false)
