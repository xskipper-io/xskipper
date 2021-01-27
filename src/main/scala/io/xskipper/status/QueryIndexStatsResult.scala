/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.status

/**
  * Case class to represent query data skipping stats
  *
  * @param status a generic status
  * @param isSkippable true if the query is skippable.
  *                    a query is considered skippable if it has indexed files
  *                    and a metadata query can be generated.
  * @param skipped_Bytes the total number of bytes skipped
  * @param skipped_Objs the total number of skipped objects
  * @param total_Bytes the total number of bytes in order to process the query
  * @param total_Objs the total number of objects in order to process the query
  */
case class QueryIndexStatsResult(var status: String,
                                 var isSkippable: Boolean,
                                 var skipped_Bytes: Long,
                                 var skipped_Objs: Long,
                                 var total_Bytes: Long,
                                 var total_Objs: Long)
{
  def update(other : QueryIndexStatsResult): Unit = {
    // The query is skippable if at least one of its data sources is skippable
    this.isSkippable ||= other.isSkippable
    // aggregate all other values
    this.skipped_Bytes += other.skipped_Bytes
    this.skipped_Objs += other.skipped_Objs
    this.total_Bytes += other.total_Bytes
    this.total_Objs += other.total_Objs
  }
}
