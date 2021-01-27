/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing.util

import org.apache.log4j.spi.LoggingEvent

import scala.util.matching.Regex

/**
  *
  * A builder class for various types of log trackers
  */
object LogTrackerBuilder {

  // creates a log tracker that tracks the skipped files
  def getRegexTracker(regex: Regex): LogTracker[String] = {

    val action = (event: LoggingEvent) => {
      val msg = event.getMessage().toString
      msg match {
        case regex(value) => value
        case _ => ""
      }
    }

    val filter = (event: LoggingEvent) => {
      val msg = event.getMessage().toString
      msg match {
        case regex(value) => true
        case _ => false
      }
    }

    new LogTracker[String](regex, filter, action)
  }
}
