/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.api


/**
  * A descriptor for a flow test.
  * a typical flow test indexes a  set datasets, then runs a query with
  * filtering and asserts the correct objects are skipped.
  * the query result is also compared to the result obtained w/o skipping.
  * optionally, the test can include an update for one or more datasets - for each
  * dataset with an updated location(also referred to as auxiliary location),
  * the objects from the aux location will be copied to the test dir,
  * and a REFRESH will be invoked. the query will then be executed
  * again, the skipped files verified and the query result will be compared to the result obtained
  * w/o skipping
  *
  * @param inputDatasets - a sequence of [[DatasetDescriptor]] used for input
  * @param inputFormats  list of input formats to run  on.
  *                      by convention, for each format in this list,
  *                      there needs to be a directory under each [[DatasetDescriptor]]'s
  *                      input location with this format's name - this dir will used
  *                      as input for that format.
  * @param name          test name
  * @param query         the query to run
  */
case class APITestDescriptor(inputDatasets: Seq[DatasetDescriptor],
                             inputFormats: Seq[String],
                             name: String,
                             query: String)

/**
  *
  * @param inputLocation                   original input location prefix (w.o format) - the objects
  *                                        in this location
  *                                        shall not be touched!
  * @param indexTypes                      sequence of tuples of (indexType, cols) where
  *                                        indexType is the name of the index, cols is a
  *                                        comma-separated list (no whitespaces) of the columns
  *                                        for that specific index
  * @param viewName                        name of the view for this dataset
  * @param expectedSkippedFiles            sequence of expected skipped files prefixes
  *                                        the prefixes are to be written as follows:
  *                             1. they must be relative to the input location
  *                             2. the format shall not be included.
  * @param updatedInputLocation            Optional location containing new/updated objects.
  *                                        if set, the objects in this location will be copied
  *                                        to the actual location of the test (under workDir.
  * @param expectedSkippedFilesAfterUpdate Optional, the file prefixes expected to be skipped
  *                                        after the update
  */
case class DatasetDescriptor(inputLocation: String,
                             indexTypes: Seq[(String, Seq[String])],
                             viewName: String,
                             expectedSkippedFiles: Seq[String],
                             updatedInputLocation: Option[String],
                             expectedSkippedFilesAfterUpdate: Option[Seq[String]])
