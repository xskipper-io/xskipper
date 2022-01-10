<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Encrypting Indexes
When using Parquet Metadata Store, The metadata can
optionally be encrypted using Parquet Modular Encryption (PME).
This is achieved since the metadata itself is stored as a Parquet dataset,
and thus PME can be used to encrypt it.
This feature applies to all input formats, for example, a dataset stored
in CSV format can have its metadata encrypted using PME.

!!! note
    In the following sections, unless said otherwise, when referring
    to footers, columns etc., these are with respect to the metadata objects,
    and not the objects in the indexed dataset.


## Modularity
Index Encryption is modular and is granular in the following way:

- Each Index can either be encrypted (with a per-index key granularity) or left plaintext.
- Footer + object name column + partition key values:
    - The footer of the metadata object (in itself a Parquet file) contains (among other things):
        - Schema of the metadata object, which reveals the types, parameters and column names for
        all indexes collected (for example, one can learn that a Bloom Filter Index is defined on column
        `city` with false-positive probability `0.1`)
        - Full path of the original dataset (or table name in case of a Hive Metastore table)
    - The object name column stores the names of all indexed objects, and their
      modification timestamps at indexing time.
    - The partition key values (for example, when Hive Style partitioning is used in the indexed dataset)
      are automatically stored by Xskipper, each virtual column in its own dedicated column.
      
The footer + object name column + partition columns are encrypted using the same key - the "footer key",
unless Plaintext footer is specified, in that case the object name column and the partition
columns will still be encrypted, but the footer itself will be left plaintext (that is,
the plaintext footer mode of PME will be activated).
If at least 1 index is encrypted then the footer key must be set.


This granularity enables usecases where different users have access to different subsets of the indexes in the metadata.
In general, Xskipper will only try to access the indexes relevant for the query (based on the predicates in the `WHERE`  clause) - and thus
it's enough to only have access to the footer key and the keys for the relevant indexes.
Thus, with an appropriate key selection, a single copy of the metadata can be used by different users, with each user having the keys for the indexes
relevant to the columns they have access to in the data.

!!! warning
    Xskipper doesn't have a fallback mechanism for index access, so even if the user has access to a subset of the indexes
    that is able to provide some skipping, if there are usable indexes for the query which are unaccessible - Xskipper will not be able to provide
    any skipping, the query will succeed but no skipping will happen - for example,
    say we have a `MinMax` on `temp` and `ValueList` on `city` but we can only access the footer key + the key 
    for the `MinMax` and we run a query with `WHERE city = 'PARIS' AND temp < 5` - even though we could use just the `MinMax`
    to get some skipping (though probably not all) - no skipping will occur.

### How to choose keys
As mentioned before, if an index is not required for a query, Xskipper won't try to access it,
and so the only factor for whether Xskipper tries to access an index is whether it's required for a query - and NOT whether it's accessible.
thus, a good practice would be to make sure that if someone has access to a column, they will also have access to the indexes defined for that column (and of course the footer).

## Usage Flow
!!! danger
    When using index encryption, whenever a "key" is configured in any Xskipper API,
    it's always the label - **NEVER** the key itself.

In the following code examples we omit the PME configuration part as it varies between different implementations.
A demonstrational KMS example can be found [here](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#columnar-encryption)

### Index Creation Flow

<center>
![Encrypted Index Creation Flow](../img/encryption_index_creation_flow.svg)
</center>

Once the metadata is created, subsequent usages (both querying and refreshing) are 
all agnostic to the fact that this metadata is encrypted - so, except for the regular
PME configurations (KMS, auth etc.) - everything remains the same.

#### Sample 1 - Creating metadata with encrypted footer (default)

=== "Python"

    ``` python
    # configure footer key
    conf = dict([("io.xskipper.parquet.encryption.footer.key", "k1")])
    xskipper.setConf(conf)
    # adding the indexes
    xskipper.indexBuilder() \
    .addMinMaxIndex("temp", "k2") \
    .addValueListIndex("city") \
    .build(reader) \
    .show(10, False)
    ```

=== "Scala"

    ``` scala
    // set the footer key
    val conf = Map(
      "io.xskipper.parquet.encryption.footer.key" -> "k1")
    xskipper.setConf(conf)
    xskipper
      .indexBuilder()
      // Add an encrypted MinMax index for temp
      .addMinMaxIndex("temp", "k2")
      // Add a plaintext ValueList index for city
      .addValueListIndex("city")
      .build(reader).show(false)
    ```

#### Sample 2 - creating metadata with plaintext footer

=== "Python"

    ``` python
    # configure footer key
    conf = dict([("io.xskipper.parquet.encryption.footer.key", "k1"),
    ("io.xskipper.parquet.encryption.plaintext.footer", "true")])
    xskipper.setConf(conf)
    # adding the indexes
    xskipper.indexBuilder() \
    .addMinMaxIndex("temp", "k2") \
    .addValueListIndex("city") \
    .build(reader) \
    .show(10, False)
    ```
    
=== "Scala"

    ``` scala
    // set the footer key
    val conf = Map(
    "io.xskipper.parquet.encryption.footer.key" -> "k1",
    "io.xskipper.parquet.encryption.plaintext.footer" -> "true")
    xskipper.setConf(conf)
    xskipper
    .indexBuilder()
    // Add an encrypted MinMax index for temp
    .addMinMaxIndex("temp", "k2")
    // Add a plaintext ValueList index for city
    .addValueListIndex("city")
    .build(reader).show(false)
    ```
### Query Flow
When running Queries, there is no practical difference between encrypted metadata and non-encrypted metadata.
The only addition is the need to configure PME.
<center>
![Encrypted Index Query Flow](../img/encryption_query_flow.svg)
</center>

### Index Refresh Flow
!!! note
    When refreshing encrypted metadata, ALL keys used in the metadata (that is, keys to all indexes + footer key) need to be accessible.

When refreshing encrypted metadata, no action is required except for PME configurations (KMS, auth etc.)
<center>
![Encrypted Index Refresh Flow](../img/encryption_refresh_flow.svg)
</center>