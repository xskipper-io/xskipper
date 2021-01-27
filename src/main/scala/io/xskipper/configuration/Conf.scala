/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.configuration

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Represents a config entry
  * @param key the config key
  * @param defaultValue the default value
  * @param doc documentation for the config value
  * @param validationFunction (optional) validation function along with error message
  *                           to validate the value set if the value is invalid fallback
  *                           to the default value
  * @tparam T
  */
case class ConfigEntry[T](
                           key: String,
                           defaultValue: T,
                           doc: String,
                           validationFunction: Option[(T => Boolean, String)] = None)

object ConfigurationUtils extends Logging {
  /**
    * Gets a value from a map if exists or returns default value if not
    *
    * @param key key
    * @param defaultValue default value
    * @param entries map to get from
    * @tparam T Value type
    * @return the value if configured and valid, default otherwise
    */
  def getOrElseFromMap[T](key: String,
                          defaultValue: T,
                          entries: Map[String, String]): T = {
    parseValue(entries.get(key), defaultValue)
  }

  /**
    * Gets a value from a map if exists or returns default value if not
    * (overload for ConcurrentHashMap)
    *
    * @param key key
    * @param defaultValue default value
    * @param entries map to get from
    * @tparam T Value type
    * @return the value if configured and valid, default otherwise
    */
  def getOrElseFromMap[T](key: String,
                          defaultValue: T,
                          entries: ConcurrentHashMap[String, String]): T = {
    parseValue(Option(entries.get(key)), defaultValue)
  }

  /**
    * Tries to parse the given value according to the type
    * If fails, returns the default value
    *
    * @param value the value
    * @param defaultValue the default value to use if value doesn't exist
    * @tparam T Value type
    * @return the value if configured and valid, default otherwise
    */
  private def parseValue[T](value: Option[String], defaultValue: T): T = {
    value match {
      case Some(v) =>
        defaultValue match {
          case _ : Boolean => Try(v.toBoolean.asInstanceOf[T]).getOrElse(defaultValue)
          case _ : Int => Try(v.toInt.asInstanceOf[T]).getOrElse(defaultValue)
          case _ : Long => Try(v.toLong.asInstanceOf[T]).getOrElse(defaultValue)
          case _ : Double => Try(v.toDouble.asInstanceOf[T]).getOrElse(defaultValue)
          case _ => Try(v.asInstanceOf[T]).getOrElse(defaultValue)
        }
      case _ => defaultValue
    }
  }

  /**
    * Generic functions that returns the config value of the given key or the default value
    * if the config doesn't exist
    *
    * @param configEntry the config entry to be returned
    * @param entries the map of config entries to search in
    * @tparam T Value type
    * @return the config value or the default value if the config value is not defined
    */
  def getConf[T](configEntry: ConfigEntry[T],
                 entries: Map[String, String]): T = {
    val v = ConfigurationUtils.getOrElseFromMap(configEntry.key, configEntry.defaultValue, entries)
    // if the value doesn't meet the condition return default value
    validateConfigValue(v, configEntry)
  }

  /**
    * Generic functions that returns the config value of the given key or the default value
    * if the config doesn't exist
    * (overload for ConcurrentHashMap)
    *
    * @param configEntry the config entry to be returned
    * @param entries the map of config entries to search in
    * @tparam T Value type
    * @return the config value or the default value if the config value is not defined
    */
  def getConf[T](configEntry: ConfigEntry[T],
                 entries: ConcurrentHashMap[String, String]): T = {
    val v =
      ConfigurationUtils.getOrElseFromMap(configEntry.key, configEntry.defaultValue, entries)
    // if the value doesn't meet the condition return default value
    validateConfigValue(v, configEntry)
  }

  /**
    * Given a value and a ConfigEntry - validates that it meets to cofig validation function
    * If yes, return the value. Else, return the default value for the config
    *
    * @param v the value to check
    * @param configEntry the config entry to check the value against
    * @tparam T
    * @return v if the value is valid. Otherwise, the default conf value
    */
  private def validateConfigValue[T](v: T, configEntry: ConfigEntry[T]): T = {
    configEntry.validationFunction match {
      case Some(f) if !f._1(v) =>
        logWarning(s"value ${v} for ${configEntry.key} is invalid - ${f._2}," +
          s" falling back to default value - ${configEntry.defaultValue}")
        configEntry.defaultValue
      case _ => v
    }
  }
}

abstract class Configuration {
  val confEntries = new ConcurrentHashMap[String, String]()

  /**
    * Update the configuration with the following entries
    *
    * @param params a map of parameters to be set
    */
  def setConf(params: Map[String, String]): Unit = {
    confEntries.putAll(params.asJava)
  }

  // overload - getting a java map (for python module)
  // using final to avoid overloading by inherited class
  final def setConf(params: java.util.Map[String, String]): Unit = {
    setConf(params.asScala.toMap)
  }

  /**
    * Sets a new key in the configuration
    *
    * @param key the key to set
    * @param value the value associated with the key
    */
  def set(key: String, value: String): Unit = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    confEntries.put(key, value)
  }

  /**
    * Removes a key from the configuration
    *
    * @param key the key to remove
    */
  def unset(key: String): Unit = {
    confEntries.remove(key)
  }

  /**
    * clean the context for the configuration by clearing the configuration map
    * defaults are used in case the values don't exist in the map
    */
  def clearConf() : Unit = {
    confEntries.clear()
  }

  /**
    * Generic functions that returns the config value of the given key or the default value
    * if the config doesn't exist
    *
    * @param configEntry the config entry to be returned
    * @tparam T
    * @return the config value or the default value if the config value is not defined
    */
  def getConf[T](configEntry: ConfigEntry[T]): T = {
    ConfigurationUtils.getConf(configEntry, confEntries)
  }

  /**
    * Returns a map of all configurations currently set
    */
  def getConf(): java.util.Map[String, String] = {
    val res = new java.util.HashMap[String, String]()
    confEntries.entrySet().asScala.foreach(e => {
      res.put(e.getKey, e.getValue)
    })
    res
  }

  /**
    * Retrieves the value associated with the given key in the configuration
    *
    * @param key the key to lookup
    * @return the value associated with the key or null if the key doesn't exist
    */
  def get(key: String): String = {
    confEntries.get(key)
  }
}
