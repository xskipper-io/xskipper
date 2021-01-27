<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Parquet Metadatastore Spec

This is a specification for how metadata is represented in the Parquet Metadata store. We support multiple versions and document them here. 
Note that the version number is stored in metadata files in a specific column of their Spark schema.
Behind the scenes it is saved in the KV Metadata of the resulting Parquet files.
This means that the version
number can never imply metadata file locations, prefixes, directory layouts etc.,
since if one knows the version number, it means the metadata files have
already been located.

## How to maintain the version numbers
The version numbers are natural numbers.
Each release can change the version number by at most one.
In particular, if 2 (or more) changes were made to the specification but no release happened between them, they will be considered as belonging to the same version.

Note: 

1. Terminology - The terms "KV Store", "KV Metadata", "Spark Schema Metadata", despite having
different meaning usually, will be used interchangeably to mean the structure
Spark Schema Metadata, which in itself is assigned per-column.
we will use them in the context of describing 
what metadata for which column is laid out in what way.
2. We will describe the structure of the spark schema, Spark's per-column 
    metadata, and use Spark types. this will allow us to be detached 
    from how spark actually represents these structures in the Parquet schema.
 
## Format Specifications

### Version 3

This version differs from Version 2 by:

1. Configuration parameters prefix changed from `com.ibm.metaindex` to `io.xskipper`
2. The index parameters are now stored as a map

### Version 2 

This version differs from Version 1 by:

1. Column name generation:  
For each index $I$ (assume $I$ is the name), defined on columns $c_1,...,c_n$ (in this order),  
for each $c_i$, let $c'_i$ denote $c_i$ with the following changes, in this order:
    * Replace all `#` with `##`
    
    * Replace all `.` with `$#$`
The column name will be:  $c'_1c'_2...c'_n\_I$len(c'_1)-len(c'_2)-...-len(c'_n)

     > That is, the transformed column names concatenated, followed by the index name,
      followed by the lengths of the transformed column names concatenated with `-` as the delimiter
     Example of such transformation:
     `SomeIndex on "lat#_.$_new" and  "$_lng.#"` will get `lat##_$#$$_new_$_lng$#$##_someindex_14-10`
     since `"lat#_.$_new"` will be mapped to `"lat##_$#$$_new"` with length 14, and `"$_lng.#"` will be mapped to `"$_lng$#$##"` with length 10

### Version 1
 
This version differs from version 0 by:

 * the addition of PME Support.
 * changes to the way column names are constructed from indexes
 * moving `tableIdentifier` metadata field to be under the `obj_name` column
 * Min/Max index is saved as nested field with native parquet types
 * Value List is saved as parquet array type

Notes:

 1. all additions were made in order for our library to be able to regenerate the encryption config (e.g., for refresh or during compaction).  
 these configs are not used by PME itself (PME uses other fields in the parquet file itself, these are not available to us via spark).  
 Theoretically it is possible to create a parquet file in which our metadata indicates an encryption config completely different than the one with which it's actually encrypted. we should avoid that.

 2. *IMPORTANT* no actual key material is ever written to the KV store, it's ALWAYS labels (e.g. `encryption.column.keys`)

 3. As of version 0 (and 1), the set of column names in spark schema for all indexes
 is prefix free. this must remain the case, as the spark column names are used by our lib to
 derive the set of columns in the parquet schema for a specific index (e.g., a `UDT` translated to a column
 with a different name in the parquet schema).

Changes:

1. Additions to `obj_name` metadata:
    If the metadata is not encrypted, then no additions are made.
    If at least 1 index is encrypted, encryption metadata will be added the following way:
    - key `encryption` of type `spark.sql.types.Metadata`, pointing to a metadata with
    with the following structure:
       - key `encryption.column.keys` pointing to a String, containing the key list string for PME.
       the format of this string matches the format for the config with the same name used in PME. see [this](https://cloud.ibm.com/docs/AnalyticsEngine?topic=AnalyticsEngine-parquet-encryption&locale=en)
       - *optional* key "encryption.plaintext.footer" of type String, containing one of `{true, false}`, indicating whether or not plaintext footer is used
       if this key is not defined, then plaintext footer is implicitly disabled.
       - key `encryption.footer.key` of type String, containing the footer key label (footer master Key ID in PME Terminology).
       this key is also used to encrypt the footer (the footer is always encrypted if encryption is on)
       this field is mandatory as a footer key is necessary if we use PME, even if plaintext
       footer mode is in use (the footer key is used only for signing in this case, and of course for `obj_name`).
       
2. Additions to each index metadata:
    Indexes which are not encrypted remain unchanged.
    For encrypted indexes, the following is added:
    - key `key_metadata` of type String, pointing to the label of the key used to encrypt
    the set of columns for this index. 
    The set of columns for a specific is obtained by acquiring the Parquet schema tree,
    and taking all the paths to leaves which start with the Spark column name for this index
    (this is why the set of spark column names must be prefix free).
    for example, for a `MinMax` on `temp`,
    the set of columns we need to encrypt is `{temp_minmax.metadata}`
    
    Note that this key label must be consistent with the one with which the columns for this index are encrypted,  
    as configured in the column key list string in the `obj_name` metadata. if they are inconsistent, then this is a bug in the lib.
    the column key list string is kept in the `obj_name` metadata to save unnecessary scans
    to re-create that config when refreshing/compacting. the `key_metadata` in each index is used
    e.g. when listing existing indexes (to be able to retrieve the `keyMetadata` field in the `Index` case class).
    
3. Column names for indexes are generated the same as in version 0, but the delimiter is now `_` 
   and not `:`, so for example, a `SomeIndex` over `a,b` would have gotten the column name `a:b_someindex` in version 0, now gets the column name `a_b_someindex`
   
4. `tableIdentifier` metadata field is now saved only under the `obj_name` column (removed from index columns).

5. Min/Max index is saved by having a nested field with `min` and `max` subfields each containing the value in native parquet type.

6. Value List index is saved by saving an array data type

### Version 0 

The metadata is represented by one row for each object, with the object 
name in its own column, and the metadata for each index in its own column as well,
with the actual content of the metadata being the serialization of the UDT
for this specific Metadata type.

1. `obj_name` column:
    stores the object name.
    * For Unversioned files, defined as
        ```scala
         StructField("obj_name", StringType, false)
        ```
         > That is, a non-nullable column named "obj_name"
                 of type String, without metadata.
    * For Versioned files (that is, version 0), defined as:
        ```scala
        val objNameMeta =  new sql.types.MetadataBuilder()
                       .putLong("version", 0)
                       .build()
            StructField("obj_name", StringType, false, objNameMeta)
        ```
        > That is, a non-nullable column named "obj_name"
         of type String, with Metadata containing a single key, "version", 
         that points to a Long
2. Per-Index Columns:

    For each index $I$ (assume $I$ is the name), defined on columns $c_1,...,c_n$ (in this order),  
    with UDT type T and params given as

    ```scala 
    params : Map[String, String] 
    ```
   
   A column with the following properties is defined:
   
   - `name`: $c_1,...,c_n$_$I$
   > that is, the column names concatenated with a ":" delimiter, followed by 
   the index name concatenated with `_`
   - `dataType`: `T`
   > That is, the UDT associated with this index.
   - `nullable` - `true`
   - `metadata`, a single key named `index`, pointing to another `spark.sql.types.Metadata`  
   with the following structure:
   
        * key `cols`, pointing to a `java.lang.String[]` containing the index columns.
        * key `name`, pointing to the String `I`
        * key `tableIdentifier`, pointing to String generated from the URI in the following manner:
            - if the URI's Scheme is `COS`, then `<bucket_name>/<object_name>`
            - else, if the path for this URI (obtained by `new URI(uri).getPath()`)
             starts with a "/", the the preceding / is trimmed from this path, else it's unchanged.
        * Optional if `params` is not empty, then a key `params` points to `params`.

### Unversioned Files

The Layout of the KV Store had several incarnations before it was versioned, so if looking at a metadata file (or group of files) without a version number, 
we will implicitly treat them as version 0, which will act as  the "as-built drawing" for the KV Store layout, as of the time the version number was introduced.  
It's not defined what will happen should we encounter a file without a version, with KV Layout other than version 0.
