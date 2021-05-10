/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.utils

import java.util.Locale
import io.xskipper.XskipperException
import io.xskipper.index.{Index, IndexCompanion}
import io.xskipper.status.Status
import io.xskipper.utils.identifier.Identifier
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField, _}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{SQLDate, SQLTimestamp}
import org.apache.spark.sql.catalyst.{InternalRow, util => CatalystUtils}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, FileScan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataTypes, DateType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import scala.reflect.runtime.universe._

object Utils extends Logging {
  // the Identifier instance to be used
  var identifier: Identifier = new Identifier()

  /**
    * Given a URI/table identifier returns the table identifier
    * that will be used in xskipper
    *
    * @param uri the uri to be parsed
    * @return The corresponding Table identifier returns it as it is
    */
  def getTableIdentifier(uri: String): String = {
    identifier.getTableIdentifier(uri)
  }

  /**
    * Given a FileStatus return the name associated with this path
    */
  def getFileId(fs: FileStatus): String = {
    identifier.getFileId(fs)
  }

  /**
    * Custom logic to be used to rename the paths which are displayed
    * in the xskipper output [[DataFrame]]-s
    *
    * @param uri the path to be displayed
    */
  def getPathDisplayName(uri: String): String = {
    identifier.getPathDisplayName(uri)
  }

  /**
    * Custom logic to be used to rename the tid which are displayed
    * in the xskipper output [[DataFrame]]-s
    *
    * @param tid the path to be displayed
    */
  def getTableIdentifierDisplayName(tid: String): String = {
    identifier.getTableIdentifierDisplayName(tid)
  }

  /**
    * Receives a string and tries to convert it to [[org.apache.hadoop.fs.Path]]
    *
    * @param path the path to convert
    * @return [[org.apache.hadoop.fs.Path]] if conversion succeeded or exception else
    */
  def stringToPath(path: String) : Path = {
    try {
      new Path(path)
    } catch {
      case e: Exception => throw new Exception("Given path is an invalid path", e)
    }
  }

  /**
    * Creates a scala object instance using the object name (using reflection)
    *
    * @param clsName the full name of the object
    * @return the object if succeeded , None otherwise
    */
  def getObjectInstance[T](clsName: String): Option[T] = {
    try {
      val mirror = runtimeMirror(getClass.getClassLoader)
      val module = mirror.staticModule(clsName)
      val obj: T = mirror.reflectModule(module).instance.asInstanceOf[T]
      logInfo(s"Loaded ${clsName} dynamically successfully")
      Some(obj)
    }
    catch {
      case e : Throwable =>
        logWarning(s"Failed loading ${clsName} dynamically, got ${e}")
        None
    }
  }

  /**
    * Creates a scala class instance with empty constructor (using reflection)
    *
    * @param clsName the full name of the object
    * @return a class instance if succeeded , None otherwise
    */
  def getClassInstance[T](clsName: String): Option[T] = {
    try {
      val mirror = runtimeMirror(getClass.getClassLoader)
      val cclass = mirror.staticClass(clsName)
      val cm = mirror.reflectClass(cclass.asClass)
      val ctorm = cm.reflectConstructor(cclass.asClass.primaryConstructor.asMethod)
      val obj = ctorm().asInstanceOf[T]
      logInfo(s"Loaded ${clsName} dynamically successfully")
      Some(obj)
    }
    catch {
      case _ : Throwable =>
        logWarning(s"Failed loading ${clsName} dynamically")
        None
    }
  }

  def createCustomIndex(idxClsName: String,
                        cols: Seq[String],
                        keyMetadata: Option[String],
                        params: Map[String, String]): Index = {
    try {
      val mirror = runtimeMirror(getClass.getClassLoader)
      val module = mirror.staticModule(idxClsName)
      val cm = mirror.reflectModule(module)
      val inst = cm.instance.asInstanceOf[IndexCompanion[_ <: Index]]
      val res = inst.apply(params, keyMetadata, cols)
      logInfo(s"Loaded ${idxClsName} dynamically successfully")
      res
    }
    catch {
      case e : Throwable =>
        throw new XskipperException(s"Failed loading ${idxClsName} dynamically, got ${e}")
    }
  }

  /**
    * Determines whether or not the column name is valid for indexing.
    * For now, a name is legal if it's a legal spark column name + no vertex
    * along the column's schema tree path contains a "."
    * for example, "`a.b`.c" is illegal (the path is [a.b, c]), but "a.b.c" ([a,b,c]) is legal.
    *
    * @param colName the column name to check
    * @return true if the column name is valid, false otherwise
    */
  def isColumnNameValid(colName: String): Boolean = {
    !UnresolvedAttribute.parseAttributeName(colName).exists(_.contains("."))
  }

  /**
    * Extracts the partition columns from a [[DataFrame]]
    *
    * @param df the [[DataFrame]] for which the partition
    *           columns are to be extracted
    * @return StructType representing the column schema
    */
  def getPartitionColumns(df: DataFrame): Option[StructType] = {
    /**
      * HACK ALERT (sort of...)
      * at the moment, the order of the virtual columns in hfs.location.partitionSchema or
      * scan.readPartitionSchema
      * is the same as their order in the object name (i.e., the closer it is to the root,
      * the lower its index in partitionSchema will be).
      */
    // Note using optimized plan to force resolving
    df.queryExecution.optimizedPlan match {
      case LogicalRelation(hfs: HadoopFsRelation, _, _, _) =>
        hfs.partitionSchemaOption
      case DataSourceV2ScanRelation(_, scan: FileScan, _) =>
        Some(scan.fileIndex.partitionSchema)
      case _ => None
    }
  }

  /**
    * Returns the [[DataFrame]] associated with the given table identifier
    *
    * @param spark [[SparkSession]] instance
    * @param tableIdentifier the table identifier requested
    * @return the [[DataFrame]] associated with the given table identifier
    */
  def getTable(spark: SparkSession, tableIdentifier: String) : DataFrame = {
    try {
      // verify the table is not a view - indexing on views is not available
      val tbl = spark.catalog.getTable(tableIdentifier)
      if (tbl.tableType == CatalogTableType.VIEW.name) {
        throw new XskipperException(s"""${Status.INDEX_VIEW_ERROR}""")
      }
      spark.table(tableIdentifier)
    }
    catch {
      case _: ParseException =>
        throw new XskipperException(Status.invalidTableIdentifier(tableIdentifier))
      case _: AnalysisException =>
        throw new XskipperException(Status.tableNotFoundError(tableIdentifier))
    }
  }

  /**
    * Checks if the given schema is valid
    * The schema is considered valid if it doens't contain column names with `.`
    *
    * @param schema the schema to check
    * @return true if the schema is valid, false otherwise
    */
  def isSchemaValid(schema: StructType): Boolean = {
    try {
      schema.foreach(field => getSchemaFields(field))
    }
    catch {
      case _: XskipperException =>
        return false
    }

    true
  }

  /**
    * Returns schema fields recursively
    *
    * @param field schema root field
    * @return (field name in lower case, (field name, field))
    * @throws XskipperException if schema field name contains a dot
    */
  def getSchemaFields(field: StructField,
                      prefix: String = ""): Seq[(String, (String, StructField))] = {
    // Make sure column name does not contain dot
    // (dots are only allowed as separators for nested StructType)
    if (field.name.contains('.')) {
      throw new XskipperException(Status.invalidColNameError)
    }

    def doConcat(a: String, b: String): String = Seq(a, b).filter(_.nonEmpty).mkString(".")

    field.dataType match {
      case nested: StructType =>
        nested.flatMap(subf =>
          getSchemaFields(subf, doConcat(prefix, field.name)))
      case _ =>
        val fName = doConcat(prefix, field.name)
        Seq((fName.toLowerCase(Locale.ROOT), (fName, field)))
    }
  }


  /**
    * Returns the expression string (full column name),
    * flattened, without any quoting or backticks added.
    * NOTE: because backticks are removed, these is a possibility of collision
    * for example, expression that select columns
    * "a.b" and "`a.b` will return the same name here!
    *
    * @param field the expression to get name for
    */
  def getName(field: Expression): String = {
    CatalystUtils.toPrettySQL(field)
  }

  /**
    * Checks if an expression is a valid single-column selection
    * a valid single-column selection is an expression that selects
    * a single column that is either flat (Attribute) or a nested column
    * that is not a part of an array.
    * @param expr
    * @return
    */
  def isExpressionValidForSelection(expr: Expression): Boolean = {
    expr match {
      case e: GetStructField => true
      case e: Attribute => true
      case _ => false
    }
  }

  /**
    * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
    */
  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MB, because when no units are specified the unit
    // is assumed to be bytes
    (JavaUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }

  /**
    * Casts an InternalRow with a given schema to a Sequence of java/scala values
    * @param row the row to cast
    * @param schema the schema corresponding to the row
    * @return a sequence of java values corresponding to the row
    */
  def toSeq(row: InternalRow, schema: StructType): Seq[Any] = {
    val len = row.numFields
    val fieldTypes = schema.map(_.dataType)
    assert(len == fieldTypes.length)

    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      fieldTypes(i) match {
        case dt: DateType =>
          values(i) = DateTimeUtils.toJavaDate(row.get(i, dt).asInstanceOf[SQLDate])
        case dt: StringType => values(i) = row.get(i, dt).toString
        case dt: TimestampType =>
          values(i) = DateTimeUtils.toJavaTimestamp(row.get(i, dt).asInstanceOf[SQLTimestamp])
        case _ => values(i) = row.get(i, fieldTypes(i))
      }

      i += 1
    }
    values
  }
}
