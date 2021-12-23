/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import java.io.{InputStream, ObjectInputStream, ObjectStreamClass}

/**
  * an [[ObjectInputStream]] that is agnostic to the serialized SUID.
  * This shall only be used as a workaround for loading the BloomFilter from
  * serialized form.
  *
  * @param in
  */
class SUIDAgnosticObjectInputStream(in: InputStream) extends ObjectInputStream(in) {
  override def readClassDescriptor(): ObjectStreamClass = {
    val inputClassDescriptor = super.readClassDescriptor()
    // scalastyle:off classforname
    val clazz = Class.forName(inputClassDescriptor.getName)
    // scalastyle:on classforname
    val res = ObjectStreamClass.lookup(clazz)
    res
  }

}
