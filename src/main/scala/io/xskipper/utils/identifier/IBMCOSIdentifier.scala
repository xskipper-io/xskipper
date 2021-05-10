/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.utils.identifier

import java.io.IOException
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.FileStatus

class IBMCOSIdentifier extends Identifier {
  /**
    * Given a URI/table identifier returns the table identifier
    * that will be used in xskipper
    *
    * @param uri the uri to be parsed
    * @return For COS the path without the service - <bucket_name>/<object_name>
    *         For Table identifier returns it as it is
    */
  override def getTableIdentifier(uri: String): String = {
    getIdentifier(uri)
  }

  /**
    * Given a FileStatus return the name associated with this path
    * (Enables for example stripping the service name from COS path)
    */
  override def getFileName(status: FileStatus): String = {
    getIdentifier(status.getPath.toUri)
  }

  private def getIdentifier(uriString: String): String = {
    getIdentifier(new URI(uriString))
  }
  private def getIdentifier(uri: URI): String = {
    val path = uri.getPath
    // remove trailing slash - as table identifier won't contain slashes in the end
    var len = path.length
    if (path.endsWith("/")) {
      len -= 1
    }
    uri.getScheme match {
      // for cos table identifier is <bucket_name>/<object_name> excluding the service name
      case "cos" =>
        // Note: Relies on Stocator - update if Stocator changes
        val host = getHost(uri)
        val bucketName = getContainerName(host)
        s"cos://${bucketName}${path.substring(0, len)}"
      // for all other cases table identifier is <object_name>
      case _ =>
        path.substring(0, len)
    }
  }

  /**
    * Extract host name from the URI
    *
    * @param uri object store uri
    * @return host name
    * @throws IOException if error
    */
  @throws[IOException]
  def getHost(uri: URI): String = {
    var host = uri.getHost
    if (host != null) return host
    host = uri.toString
    val sInd = host.indexOf("//") + 2
    host = host.substring(sInd)
    val eInd = host.indexOf("/")
    if (eInd != -1) host = host.substring(0, eInd)
    host = URLDecoder.decode(host, StandardCharsets.UTF_8.toString)
    host
  }

  /**
    * Extracts container name from the container.service or container
    *
    * @param hostname        hostname to split
    * @return the container
    * @throws IOException if hostname is invalid
    */
  @throws[IOException]
  def getContainerName(hostname: String): String = {
    val i = hostname.lastIndexOf(".")
    if (i <= 0) {
      return hostname
    }
    hostname.substring(0, i)
  }
}
