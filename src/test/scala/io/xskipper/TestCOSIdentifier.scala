/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

import io.xskipper.utils.Utils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.FunSuite

class TestCOSIdentifier extends FunSuite {

  val params = Map[String, String](
    "io.xskipper.identifierclass" -> "io.xskipper.utils.identifier.IBMCOSIdentifier")
  Xskipper.setConf(params)

  test("getTableIdentifier") {
    val URIs = Seq(
      "cos://testbucket.service/",
      "cos://testbucket.service",
      "cos://testbucket.service/myobject/myobject2",
      "cos://my-bucket-read.service_a-job-id_1/name/with/prefix/my-object-read",
      "cos://my-bucket-read.service_a-job-id_1/name/with/prefix/my-object-read/",
      "/Users/abc/def",
      "/Users/abc/def/")
    val expectedTableIdentifiers = Seq(
      "cos://testbucket",
      "cos://testbucket",
      "cos://testbucket/myobject/myobject2",
      "cos://my-bucket-read/name/with/prefix/my-object-read",
      "cos://my-bucket-read/name/with/prefix/my-object-read",
      "/Users/abc/def",
      "/Users/abc/def")

    // verify tableIdentifiers
    (URIs zip expectedTableIdentifiers).foreach { case (path: String, tid: String) =>
      assert(Utils.getTableIdentifier(path) == tid)
    }
  }

  test("getFileID") {
    val URIs = Seq(
      "cos://testbucket.service/a.parquet",
      "cos://testbucket.service/myobject/myobject2/a.parquet",
      "cos://my-bucket-read.service_a-job-id_1/name/with/prefix/my-object-read/a.parquet",
      "cos://my-bucket-read.service_a-job-id_1/name/with/prefix/my-object-read/a.parquet",
      "/Users/abc/def/a.parquet",
      "/Users/abc/def/a.parquet")
    val expectedFileIds = Seq(
      "cos://testbucket/a.parquet#0",
      "cos://testbucket/myobject/myobject2/a.parquet#0",
      "cos://my-bucket-read/name/with/prefix/my-object-read/a.parquet#0",
      "cos://my-bucket-read/name/with/prefix/my-object-read/a.parquet#0",
      "/Users/abc/def/a.parquet#0",
      "/Users/abc/def/a.parquet#0")

    // verify tableIdentifiers
    (URIs zip expectedFileIds).foreach { case (path: String, fid: String) =>
      val fs = new FileStatus()
      fs.setPath(new Path(path))
      assert(Utils.getFileId(fs) == fid)
    }
  }
}
