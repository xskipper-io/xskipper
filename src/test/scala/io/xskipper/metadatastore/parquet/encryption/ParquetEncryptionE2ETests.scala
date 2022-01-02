package io.xskipper.metadatastore.parquet.encryption

import io.xskipper.metadatastore.parquet.{Parquet, ParquetMetadataHandle, ParquetXskipperProvider}
import io.xskipper.testing.util.Utils
import io.xskipper.testing.util.Utils.strToFile
import io.xskipper.{Registration, Xskipper}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{TrueFileFilter, WildcardFileFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.keytools.KeyToolkit
import org.apache.parquet.hadoop.ParquetFileWriter.{EFMAGIC, MAGIC}
import org.apache.spark.SparkException
import org.scalatest.FunSuite
import org.scalatest.Matchers.{be, noException}

import java.io.{File, FileInputStream, IOException}
import java.nio.file.Files

abstract class ParquetEncryptionE2ETests(override val datasourceV2: Boolean = false)
  extends FunSuite with ParquetXskipperProvider {
  Registration.setActiveMetadataStoreManager(Parquet)

  val hconf = spark.sparkContext.hadoopConfiguration

  // KMS class names
  val kmsClientClassName = "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS"
  val cryptoFactoryClassName = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"

  // key configs to use in tests
  val keySetA = Map("k1" -> "rnZHCxhUHr79Y6zvQnxSEQ==", "k2" -> "LjxH/aXxMduX6IQcwQgOlw")
  // inverting the keys so a read of file encrypted with keySetA will fail
  val keySetB = Map("k1" -> "LjxH/aXxMduX6IQcwQgOlw", "k2" -> "rnZHCxhUHr79Y6zvQnxSEQ==")

  val baseDir = Utils.concatPaths(System.getProperty("user.dir"), "src/test/resources")
  val baseInputPath = Utils.concatPaths(baseDir, "input_datasets/gridpocket/initial/parquet")
  val baseUpdatePath = Utils.concatPaths(baseDir, "input_datasets/gridpocket/updated/parquet")

  // copy input to temp directory to allow update
  val input = Files.createTempDirectory("xskipper_encryption_validation").toString
  input.deleteOnExit()
  FileUtils.copyDirectory(baseInputPath, input, false)

  // test with plain footer
  testEncryptedMetadata(true)
  // test with encrypted footer
  testEncryptedMetadata(false)

  def testEncryptedMetadata(plainTextFooter: Boolean): Unit = {
    test(s"test metadata encryption plainTextFooter = ${plainTextFooter.toString}") {
      resetXskipper()
      clearEncryptionConfig(hconf)

      // set JVM wide metadatastore parameters
      val jvmParameters = new java.util.HashMap[String, String]()
      jvmParameters.put("io.xskipper.parquet.encryption.footer.key", "k1")
      jvmParameters.put("io.xskipper.parquet.encryption.plaintext.footer", plainTextFooter.toString)
      Xskipper.setConf(jvmParameters)

      setEncryptionConfig(hconf, keySetA)

      // index the dataset
      val reader = spark.read.format("parquet")
      val xskipper = getXskipper(input)

      if (xskipper.isIndexed()) {
        xskipper.dropIndex()
      }

      xskipper.indexBuilder()
        .addMinMaxIndex("temp", "k1")
        .addBloomFilterIndex("city", "k1")
        .addValueListIndex("vid", "k2")
        .build(reader)

      // get metadata path from xskipper instance
      val metadataPath = xskipper.metadataHandle()
        .asInstanceOf[ParquetMetadataHandle]
        .getMDPath().path.toString

      // clear encryption config and set new key set
      resetAndSetEncryptionConfig(hconf, keySetB)
      // assert the metadata is encrypted
      assertEncrypted(metadataPath, plainTextFooter)

      // assert we can read the metadata when the keys are present
      resetAndSetEncryptionConfig(hconf, keySetA)
      noException should be thrownBy spark.read.parquet(metadataPath).show()

      // check refresh
      FileUtils.copyDirectory(baseUpdatePath, input, false)
      val buildRes = xskipper.refreshIndex(reader)
      assert(Utils.isResDfValid(buildRes))

      // clear encryption config and set new key set
      resetAndSetEncryptionConfig(hconf, keySetB)
      // assert the metadata is encrypted
      assertEncrypted(metadataPath, plainTextFooter)

      // assert we can read the metadata when the keys are present
      resetAndSetEncryptionConfig(hconf, keySetA)
      noException should be thrownBy spark.read.parquet(metadataPath).show()
    }
  }

  /**
    * given a path to a folder containing parquet files checks whether all
    * @param path a path to a folder containing encrypted parquet files
    * @param plainTextFooter if the parquet doesn't have plain text footer
    *                        the check will also verify the encryption by checking the magic number
    */
  def assertEncrypted(path: String, plainTextFooter: Boolean): Unit = {
    // try reading the file using spark and assert it fails on reading the footer
    // get the metadata path and try reading the file
    val caught = intercept[SparkException] {
      spark.read.parquet(path)
    }
    assert(caught.getMessage.contains("Could not read footer for file"))

    // for encrypted footer we can also check:
    // 1. The exception complains on GCM tag failure
    // 2. The parquet magic is `PARE`
    if (!plainTextFooter) {
      assert(caught.getMessage.contains("GCM tag check failed"))

      // Check the parquet MAGIC for each file
      FileUtils.listFiles(new File(path),
        new WildcardFileFilter("*.parquet"),
        TrueFileFilter.INSTANCE).forEach {
        f =>
          val magic = new Array[Byte](MAGIC.length)
          try {
            val is = new FileInputStream(f.toString)
            try {
              assertResult(magic.length)(is.read(magic))
              assertResult(EFMAGIC)(magic)
            } catch {
              case e: IOException =>
                throw new Exception("Failed to read magic string at the beginning of file", e)
            } finally if (is != null) is.close()
          } catch {
            case e: Exception => throw new Exception("Failed to open file input stream", e)
          }
      }
    }
  }

  /**
    * Sets parquet encryption config in the given config parameter
    * @param hconf the hadoop config to set parquet encryption keys in
    * @param keys the keys to use in the form of map key, value.
    *                if no value is given a set of dummy keys will be used
    */
  def setEncryptionConfig(hconf: Configuration,
                          keys: Map[String, String]): Unit = {
    hconf.set("parquet.encryption.kms.client.class", kmsClientClassName)
    hconf.set("parquet.crypto.factory.class", cryptoFactoryClassName)
    hconf.set("parquet.encryption.key.list",
      keys.map(tuple => s"${tuple._1}:${tuple._2}").mkString(","))
  }

  /**
    * Clears any existing parquet encryption keys in the JVM and given config
    * @param hconf the hadoop config to remove parquet encryption keys from
    */
  def clearEncryptionConfig(hconf: Configuration): Unit = {
    hconf.unset("parquet.encryption.key.list")
    KeyToolkit.removeCacheEntriesForAllTokens()
  }

  /**
    * Resets the parquet encryption config and set the given keys
    * @param hconf the hadoop config to remove parquet encryption keys from
    * @param keys the new keys to use
    */
  def resetAndSetEncryptionConfig(hconf: Configuration,
                                  keys: Map[String, String]): Unit = {
    clearEncryptionConfig(hconf)
    setEncryptionConfig(hconf, keys)
  }

  def resetXskipper(): Unit = {
    Xskipper.reset(spark)
    Registration.setActiveMetadataStoreManager(Parquet)
  }
}

class ParquetEncryptionE2ETestsV1 extends ParquetEncryptionE2ETests(false)

class ParquetEncryptionE2ETestsV2 extends ParquetEncryptionE2ETests(true)
