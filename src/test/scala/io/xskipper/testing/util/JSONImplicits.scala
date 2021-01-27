/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing.util

import org.json.simple._

object JSONImplicits {

  implicit class JSONImplicits(jsonObj: JSONObject) {

    def getAs[T](key: String): T = {
      if (!jsonObj.containsKey(key)) {
        throw new IllegalArgumentException(s"key $key does not exist in object \n $jsonObj")
      }
      jsonObj.get(key).asInstanceOf[T]
    }

    def getOpt[T](key: String): Option[T] = {
      jsonObj.containsKey(key) match {
        case true => Some(getAs[T](key))
        case false => None
      }
    }

    def getOrElse[T](key: String, default: T): T = {
      getOpt[T](key).getOrElse(default)
    }

    def getSeq[T](key: String): Seq[T] = {
      val jsonArray = getAs[JSONArray](key)
      (0 until jsonArray.size()).map(jsonArray.get(_).asInstanceOf[T])
    }

    def getOptionSeq[T](key: String): Option[Seq[T]] = {
      jsonObj.containsKey(key) match {
        case true => Some(getSeq[T](key))
        case false => None
      }
    }

    def getLong(key: String): Long = getAs[Long](key)

    def getBoolean(key: String): Boolean = getAs[Boolean](key)

    def getString(key: String): String = getAs[String](key)

  }

}

