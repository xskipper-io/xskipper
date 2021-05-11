/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.utils.identifier

import java.net.URI

import org.apache.hadoop.fs.FileStatus

class Identifier {
  /**
    * Given a URI/table identifier returns the table identifier
    * that will be used in xskipper
    */
  def getTableIdentifier(uri: URI): String = getIdentifier(uri)

  /**
    * Given a FileStatus returns the identifier that will be used as the fileID
    */
  def getFileId(fs: FileStatus): String = s"${getFileName(fs)}#${fs.getModificationTime}"

  /**
    * Given a FileStatus return the name associated with this path
    */
  def getFileName(status: FileStatus): String = getIdentifier(status.getPath.toUri)

  /**
    * Custom logic to be used to rename the paths which are displayed
    * in the xskipper output [[DataFrame]]-s
    * @param path the path to be displayed
    */
  def getPathDisplayName(path: String): String = path

  /**
    * Custom logic to be used to rename the table identifiers which are displayed
    * in the xskipper output [[DataFrame]]-s
    * @param tid the tid to be displayed
    */
  def getTableIdentifierDisplayName(tid: String): String = tid

  private def getIdentifier(uri: URI): String = {
    val path = uri.getPath
    // remove trailing slash - as table identifier won't contain slashes in the end
    var len = path.length
    if (path.endsWith("/")) {
      len -= 1
    }
    path.substring(0, len)
  }
}
