/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing.util

import org.apache.log4j._
import org.apache.log4j.spi.LoggingEvent

import scala.util.matching.Regex

class LogTracker[T](
    regexp: Regex,
    filter: LoggingEvent => Boolean,
    action: LoggingEvent => T)
  extends AppenderSkeleton {
  val rootLogger = Logger.getRootLogger
  var started = false
  private val resultSet = scala.collection.mutable.Set.empty[T]


  def getResultSet(): Set[T] = resultSet.toSet

  override def append(event: LoggingEvent): Unit = {
    // filtering and adding to set after applying action
    if (filter(event)) {
      resultSet.add(action(event))
    }
  }

  override def close(): Unit = {
  }

  // attaches filter to logger
  def startCollecting(): Unit = {
    if (!started) {
      started = true
      rootLogger.addAppender(this)
    }
  }

  // detaches filter to logger
  def stopCollecting(): Unit = {
    if (started) {
      started = false
      rootLogger.removeAppender(this)
    }
  }

  // clears result set
  def clearSet(): Unit = {
    // empty set
    resultSet.clear()
  }

  override def requiresLayout(): Boolean = false
}
