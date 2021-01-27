<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Metadata Versioning

The versioning mechanism, version numbers and inter-version support matrix is up to the metadata store implementation.  
However, some details were pulled above the metadata store abstraction.  

A metadata store must be able to report the **_metadata version status_**, which is defined as follows:

```scala
object MetadataVersionStatus extends Enumeration {
  type MetadataVersionStatus = Value

  /**
    * The stored metadata is from exactly the same version
    * as the metadata version of this jar.
    */
  val CURRENT = Value("current")
  
  /**
    * The stored metadata is from a version strictly smaller than
    * the metadata version of this jar. however, it can be used for filtering
    * and a REFRESH operation will result in the metadata having the `CURRENT` status
    */
  val DEPRECATED_SUPPORTED = Value("deprecated_supported")

  /**
    * The stored metadata is from a version strictly smaller than
    * the metadata version of this jar, a version so old it can't be used for filtering.
    * a REFRESH operation may or may not be able to upgrade it, up to the
    * metadata store's `isMetadataUpgradePossible` method.
    */
  val DEPRECATED_UNSUPPORTED = Value("deprecated_unsupported")

  /**
    * The stored metadata is from a version which is strictly greater
    * than the metadata version of this jar.
    * the metadata store is not expected to able to either read or refresh this metadata.
    */
  val TOO_NEW = Value("too_new")
}
```
and so the metadata store must be able to perform the following operation regarding versioning:
1. Report the Metadata Version Status:
      ```scala
      def getMdVersionStatus(): MetadataVersionStatus
      ```
2. Report whether or not a metadata upgrade is possible:
      ```scala
      def isMetadataUpgradePossible(): Boolean
      ```
3. Perform a Metadata Upgrade if reported as possible:
      ```scala
      def upgradeMetadata(): Unit
      ```

During a REFRESH operation, the metadata version status is first checked, and upgraded if necessary, before performing the actual refresh or removing metadata for deleted objects.  
Any failure during this process (e.g., metadata version status `TOO_NEW`, `isMetadataUpgradePossible` returning false etc.) will fail the refresh.
