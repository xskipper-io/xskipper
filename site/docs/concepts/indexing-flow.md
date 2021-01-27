<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Indexing Flow

## Definitions

### Index

An Index is a collection of data skipping metadata. In general data skipping metadata can enable skipping row or column subsets of a dataset. In our case we skip data at object/file granularity.  

### Metadata Generator
A component which generates abstract metadata by processing an object/file.   
The metadata is abstract in the sense that it's defined in memory. The concrete storage format for the metadata is implemented by the Metadata Translator.

### Metadata Translator

A component which translates the metadata created by the index to a suitable format in order to be stored in the metadatastore.
 
## Index Creation Flow

![Indexing Flow](../img/indexing-flow.png)

Index creation runs in 2 phases:

1. Generaring abstract metadata types which hold the metadata in memory.

2. Translating the abstract metadata types to a metadatastore representation and storing it.

Xskipper currently supports a parquet metadata store which stores the metadata in parquet files.
For more information about the parquet metadata store see [here](/api/developer/parquet-metadatastore-spec).
