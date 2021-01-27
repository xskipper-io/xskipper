/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.status

object Status {
  // status codes
  val SUCCESS = "SUCCESS"
  val FAILED = "FAILED"

  // index lifecycle
  val INDEX_NOT_FOUND = "Index Not Found"
  val INDEX_ALREADY_EXISTS = "Index Already Exists"
  val INDEX_REMOVED = "Index Removed"
  val INDEX_CREATED = "Index Created Successfully"
  val INVALID_INDEX_TYPE = "Invalid Index Type"
  val FAILED_REMOVE = "Failed To Remove Index"
  val FAILED_CREATION = "Failed To Create Index"
  val INVALID_INDEX_DATA_TYPE = "Data type is not supported and cannot be indexed"

  // index building
  val NO_DATA = "No Data Was Supplied"
  val MINMAX_WRONG_TYPE = "MinMax Index can only be created on non-complex data types"
  val BLOOMFILTER_WRONG_TYPE = "Bloom Filter Index can only be created on " +
    "String or Integral (Byte, Short, Integer, Long) types"
  val VALUELIST_WRONG_TYPE = "ValueList Index can only be created on non-complex data types"
  val DUPLICATE_INDEX = "Duplicate index definitions found"
  val GLOBPATH_INDEX = "Indexing of Glob paths is not supported"
  val HIVE_NON_PARTITIONED_TABLE = "Index build Failed - can't index " +
    "non-partitioned hive tables. to index such tables," +
    " index the physical location directly"
  val INDEX_VIEW_ERROR = "Index build Failed - can't index views"

  def invalidConfig(param: String): String = {s"xskipper configuration $param missing"}
  def invalidTableIdentifier(tlb: String): String = {s"$tlb Is not a valid table identifier"}
  def tableNotFoundError(tlb: String): String = {s"Table $tlb Not Found"}
  def nonExistentColumnError(colName: String): String = {s"Column $colName Does not exist"}
  def partitionColumnError(colName: String): String = {s"Column $colName is used " +
    "for partitioning and cannot be indexed"}
  def invalidColNameError: String = {s"A Dataset must not contain a column in which" +
    s" a dot exists other than for separating nested fields"}
  def encryptionNotSupportedError(handleName: String): String = {
    s"""Encryption is not supported by the current MetadataHandle $handleName"""
  }
}
