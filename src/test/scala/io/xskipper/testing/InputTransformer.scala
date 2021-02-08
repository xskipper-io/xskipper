/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing

import java.io.{File, IOException}
import java.nio.file.Files

import io.xskipper.testing.util.Utils._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.{SaveMode, SparkSession}

object InputTransformer extends Logging {

  // supported formats
  val SUPPORTED_FORMATS = Seq("csv", "json", "parquet", "orc")

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("Input Transformer")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    val inputDatasets = Seq(
      "/tmp/csv/tiny1",
      "/tmp/csv/tiny1Updated",
      "/tmp/csv/dates",
      "/tmp/csv/vallist1",
      "/tmp/csv/tiny1Na",
      "/tmp/csv/tiny1UpdatedNa",
      "/tmp/csv/tiny1Na2",
      "/tmp/csv/tiny1Multiple"
    )

    val outputLocations = Seq(
      "/tmp/testing_input/tiny1/initial",
      "/tmp/testing_input/tiny1/updated",
      "/tmp/testing_input/dates/initial",
      "/tmp/testing_input/vallist1/initial",
      "/tmp/testing_input/tiny1Na/initial",
      "/tmp/testing_input/tiny1Na/updated",
      "/tmp/testing_input/tiny1Na2/initial",
      "/tmp/testing_input/tiny1Multiple/initial"
    )
    inputDatasets.zip(outputLocations).foreach({
      case (input, dest) =>
        transformDataset(spark, input, "csv", dest, Seq("parquet", "orc", "csv", "json"))
    })
  }


  def transformDataset(spark: SparkSession,
                       inputLocation: String,
                       inputFormat: String,
                       outputLocation: String,
                       outputFormats: Seq[String]): Unit = {
    outputFormats.foreach(format => {
      transformDataset(spark,
        inputLocation,
        inputFormat,
        concatPaths(outputLocation, format),
        format)
    })
  }

  def transformDataset(spark: SparkSession,
                       inputLocation: String,
                       inputFormat: String,
                       outputLocation: String,
                       outputFormat: String): Unit = {
    assert(SUPPORTED_FORMATS.contains(inputFormat), s"$inputFormat is not identified!")
    assert(SUPPORTED_FORMATS.contains(outputFormat), s"$outputFormat is not identified!")
    val (inputSuffix: String, inputFormatReaderOptions: Map[String, String]) = inputFormat match {
      case "csv" =>
        (CSV_DEFAULT_SUFFIX, CSV_DEFAULT_READER_OPTIONS)
      case "json" =>
        (JSON_DEFAULT_SUFFIX, JSON_DEFAULT_READER_OPTIONS)
      case "parquet" =>
        (PARQUET_DEFAULT_SUFFIX, PARQUET_DEFAULT_READER_OPTIONS)
      case "orc" =>
        (ORC_DEFAULT_SUFFIX, ORC_DEFAULT_READER_OPTIONS)
    }

    val (outputSuffix: String,
    outputFormatReaderOptions: Map[String, String],
    outputFormatWriterOptions: Map[String, String]) = outputFormat match {
      case "csv" =>
        (CSV_DEFAULT_SUFFIX, CSV_DEFAULT_READER_OPTIONS, CSV_DEFAULT_WRITER_OPTIONS)
      case "json" =>
        (JSON_DEFAULT_SUFFIX, JSON_DEFAULT_READER_OPTIONS, JSON_DEFAULT_WRITER_OPTIONS)
      case "parquet" =>
        (PARQUET_DEFAULT_SUFFIX, PARQUET_DEFAULT_READER_OPTIONS, PARQUET_DEFAULT_WRITER_OPTIONS)
      case "orc" =>
        (ORC_DEFAULT_SUFFIX, ORC_DEFAULT_READER_OPTIONS, ORC_DEFAULT_WRITER_OPTIONS)
    }

    transformDataset(spark,
      inputLocation,
      inputFormat,
      inputSuffix,
      inputFormatReaderOptions,
      outputLocation,
      outputFormat,
      outputSuffix,
      outputFormatReaderOptions,
      outputFormatWriterOptions)
  }

  /**
    * Translates a Dataset from one format to another, while maintaining original layout -
    * each object in the input dataset is mapped to an object with the same
    * name (up to file extension) that contains exactly
    * the same rows the belonged to the original object.
    * NOTE REGARDING DATASETS WITH HIVE STYLE PARTITIONS:
    * if hive-style partitioning exists, the translation will be performed inside each
    * partition separately, this means that the hive-style partitioning structure
    * will also exist in the output. note, however, that datasets where some of the objects are
    * inside partitions and some are not or datasets where objects exist along
    * the partitiong path are not supported
    *
    * @param spark         [[SparkSession]] to use
    * @param inputLocation Location of the input dataset
    * @param inputFormat
    * @param inputSuffix
    * @param inputFormatReaderOptions
    * @param outputLocation
    * @param outputFormat
    * @param outputSuffix
    * @param outputFormatReaderOptions
    * @param outputFormatWriterOptions
    */
  def transformDataset(spark: SparkSession,
                       inputLocation: String,
                       inputFormat: String,
                       inputSuffix: String,
                       inputFormatReaderOptions: Map[String, String],
                       outputLocation: String,
                       outputFormat: String,
                       outputSuffix: String,
                       outputFormatReaderOptions: Map[String, String],
                       outputFormatWriterOptions: Map[String, String]): Unit = {
    if (new File(outputLocation).exists()) {
      throw new IOException(s"$outputLocation already exists!")
    }

    val baseDf = spark.read.format(inputFormat).options(inputFormatReaderOptions)
      .load(inputLocation)

    // get the FileIndex for the dataset
    val fileIndex: PartitioningAwareFileIndex = baseDf.queryExecution.logical match {
      case LogicalRelation(hfs: HadoopFsRelation, _, _, _)
        if hfs.location.isInstanceOf[PartitioningAwareFileIndex] =>
        hfs.location.asInstanceOf[PartitioningAwareFileIndex]
    }

    if (fileIndex.partitionSpec().partitions.nonEmpty) {
      // this means we have hive-style partitioning
      fileIndex.partitionSpec().partitions.foreach(partPath => {
        val origPartUri = partPath.path.toUri
        val partRelPath = origPartUri.resolve(inputLocation).relativize(origPartUri).getPath
        val newOutputLocation = concatPaths(outputLocation, partRelPath)
        transformSingleDataset(spark,
          partPath.path.toString,
          inputFormat,
          inputSuffix,
          inputFormatReaderOptions,
          newOutputLocation,
          outputFormat,
          outputSuffix,
          outputFormatReaderOptions,
          outputFormatWriterOptions)

      })
    } else {
      // no hive style partitioning, call translateSingleDataset directly
      transformSingleDataset(spark,
        inputLocation,
        inputFormat,
        inputSuffix,
        inputFormatReaderOptions,
        outputLocation,
        outputFormat,
        outputSuffix,
        outputFormatReaderOptions,
        outputFormatWriterOptions)
    }
  }

  private def transformSingleDataset(spark: SparkSession,
                                     inputLocation: String,
                                     inputFormat: String,
                                     inputSuffix: String,
                                     inputFormatReaderOptions: Map[String, String],
                                     outputLocation: String,
                                     outputFormat: String,
                                     outputSuffix: String,
                                     outputFormatReaderOptions: Map[String, String],
                                     outputFormatWriterOptions: Map[String, String]): Unit = {
    val outputFile = new File(outputLocation)
    if (outputFile.exists()) {
      throw new IOException(s"$outputLocation already exists!")
    } else {
      outputFile.mkdirs()
    }

    val inputDf = spark.read.format(inputFormat)
      .options(inputFormatReaderOptions).load(inputLocation)
    val inputFiles = inputDf.inputFiles
    val inputSchema = inputDf.schema
    inputFiles.foreach(fileName => {
      val df = spark.read.format(inputFormat)
        .options(inputFormatReaderOptions).schema(inputSchema).load(fileName)
      val origFileSimpleName = {
        val simpleName = new Path(fileName).getName
        simpleName.endsWith(inputSuffix) match {
          case true => simpleName.substring(0, simpleName.length - inputSuffix.length)
          case false =>
            throw new IllegalStateException(s"File $fileName does not end with $inputSuffix")
        }
      }
      val tmpDirFile = Files.createTempDirectory("xskipper_translation").toFile
      tmpDirFile.deleteOnExit()
      val tmpDir = new Path(tmpDirFile.toString)
      df.repartition(1)
        .write.format(outputFormat)
        .options(outputFormatWriterOptions)
        .mode(SaveMode.Append)
        .save(tmpDir.toString)
      val newFiles = spark.read.format(outputFormat)
        .options(outputFormatReaderOptions).load(tmpDir.toString).inputFiles
      assert(newFiles.length == 1)
      val tmpFile = new Path(newFiles(0)).toUri.getPath
      val newFileName = new Path(concatPaths(outputLocation, origFileSimpleName))
        .suffix(outputSuffix).toString
      logInfo(s"Translating $fileName to $newFileName")
      FileUtils.copyFile(new File(tmpFile), new File(newFileName), false)
    })
  }
}
