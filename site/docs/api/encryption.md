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
      
The footer + object name column + partition columns are encrypted using the same key - "the footer key",
unless Plaintext footer is specified, in that case the object name column and the partition
columns will still be encrypted, but the footer itself will be left plaintext (that is,
the plaintext footer mode of PME will be activated).
If at least 1 index is encrypted then the footer key must be set.


## Usage Flow
!!! danger
    When using index encryption, whenever a "key" is configured in any Xskipper API,
    it's always the label - NEVER the key itself.

### Index Creation Flow
``` mermaid
graph LR
  A[Start] --> B[Configure PME];
  B --> C[Configure footer key, (optional) plaintext footer];
  C --> D[Add indexes, attach key metadata to encrypted once];
  D --> E[Build];
  E --> F[Done]
```

### Query Flow
``` mermaid
graph LR
  A[Start] --> B[Cofigure PME];
  B --> C[Enable Xskipper];
  C --> D[Run Queries];
  D --> E[Done];
```

### Index Refresh Flow
``` mermaid
graph LR
  A[Start] --> B[Configure PME];
  B --> C[Call Refresh];
  C --> D[Done];
```