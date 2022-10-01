/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing.util

import org.apache.logging.log4j.core.LogEvent

import scala.util.matching.Regex

/**
  *
  * A builder class for various types of log trackers
  */
object LogTrackerBuilder {

  // creates a log tracker that tracks the skipped files
  def getRegexTracker(name: String, regex: Regex): LogTracker[String] = {

    val action = (event: LogEvent) => {
      val msg = event.getMessage().toString
      msg match {
        case regex(value) => value
        case _ => ""
      }
    }

    val filter = (event: LogEvent) => {
      val msg = event.getMessage().toString
      msg match {
        case regex(_) => true
        case _ => false
      }
    }

    new LogTracker[String](name, regex, action)
  }
}
