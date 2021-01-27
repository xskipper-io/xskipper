/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.status

case class RefreshResult(status: String, new_entries_added: Int, old_entries_removed: Int)
