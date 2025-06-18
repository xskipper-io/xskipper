/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.xskipper.{DataSkippingFileIndexRule, DataSkippingUtils}

package object implicits {
  /**
    * xskipper implicit class on [[SparkSession]]
    */
  implicit class XskipperImplicits(val sparkSession: SparkSession) extends AnyVal {
    /**
      * Enable xskipper by adding the necessary rules
      */
    def enableXskipper(): SparkSession = sparkSession.synchronized {
      // inject only if needed
      getDataSkippingFileIndexRule() match {
        // if the rule is already injected make sure it is enabled
        case Some(rule) => rule.enableDataSkipping
        case _ =>
          // inject rule and enable
          val rule = new DataSkippingFileIndexRule
          rule.enableDataSkipping
          DataSkippingUtils.injectRuleExtendedOperatorOptimizationRule(
            sparkSession,
            rule)
      }
    }

    /**
      * Disable xskipper by disabling the rules
      */
    def disableXskipper(): Unit = {
      // if the rule is already injected disable it otherwise do nothing
      getDataSkippingFileIndexRule() match {
        case Some(dataSkippingRule) =>
          dataSkippingRule.disableDataSkipping
        case _ =>
      }
    }

    /**
      * Checks whether xskipper is enabled
      *
      * @return true if the xskipper is enabled
      */
    def isXskipperEnabled(): Boolean = {
      getDataSkippingFileIndexRule() match {
        case Some(rule) => rule.isEnabled
        case _ => false
      }
    }

    /**
      * @return Returns the active data skipping rule in the current SparkSession if exists
      */
    private def getDataSkippingFileIndexRule(): Option[DataSkippingFileIndexRule] = {
      sparkSession.sessionState.optimizer.
        extendedOperatorOptimizationRules
        .find(_.isInstanceOf[DataSkippingFileIndexRule]) match {
        case Some(rule) => Some(rule.asInstanceOf[DataSkippingFileIndexRule])
        case _ => None
      }
    }
  }
}
