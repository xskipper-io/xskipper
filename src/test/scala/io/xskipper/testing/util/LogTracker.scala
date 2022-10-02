/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.testing.util

import org.apache.logging.log4j.core.Filter.Result
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.config.plugins.{Plugin, PluginAttribute, PluginElement, PluginFactory}
import org.apache.logging.log4j.core.filter.RegexFilter
import org.apache.logging.log4j.core.{Appender, Core, LogEvent, LoggerContext}

import scala.util.matching.Regex

object LogTracker {
  @PluginFactory
  def createAppender[T](@PluginAttribute("name") name: String,
                         @PluginAttribute("regex") regexp: Regex,
                     @PluginElement("action") action: LogEvent => T
                    ): LogTracker[T] = {
    new LogTracker[T](name, regexp, action)
  }
}

@Plugin(name = "LogTracker", category = Core.CATEGORY_NAME,
  elementType = Appender.ELEMENT_TYPE, printObject = true)
class LogTracker[T](
    name: String,
    regexp: Regex,
    action: LogEvent => T)
  extends AbstractAppender(name, RegexFilter.createFilter(regexp.regex, Array.empty
    , false, Result.ACCEPT, Result.DENY), null , true, Property.EMPTY_ARRAY) {

  val rootLogger = LoggerContext.getContext(false)
    .getConfiguration.getRootLogger

  var started = false
  private val resultSet = scala.collection.mutable.Set.empty[T]

  override def append(event: LogEvent): Unit = {
    // filtering and adding to set after applying action
    resultSet.add(action(event))
  }

  def getResultSet(): Set[T] = resultSet.toSet

  // attaches filter to logger
  def startCollecting(): Unit = {
    if (!started) {
      started = true
      rootLogger.addAppender(this, null, null)
      this.start()
    }
  }

  // detaches filter to logger
  def stopCollecting(): Unit = {
    if (started) {
      started = false
      rootLogger.removeAppender(name)
      this.stop()
    }
  }

  // clears result set
  def clearSet(): Unit = {
    // empty set
    resultSet.clear()
  }
}
