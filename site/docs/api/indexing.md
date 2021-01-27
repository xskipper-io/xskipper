<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Indexing 

For every column in the object, Xskipper can collect summary metadata. This metadata is used during query evaluation to skip over objects which have no relevant data.

## Default Indexes

The following indexes are supported out of the box:

| Index type  | Description  | Applicable to predicates in WHERE clauses  | Column types |
|------------|--------------|--------------|--------------|
| MinMax |Stores minimum and maximum values for a column | <,<=,=,>=,> | All types except for complex types. See [Supported Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html). |
| ValueList | Stores the list of unique values for the column | =,IN,LIKE | All types except for complex types. See [Supported Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).|
| BloomFilter | Uses a bloom filter to test for set membership | =,IN | Byte, String, Long, Integer, Short |

In order to add an index using the `IndexBuilder` to specify the required indexes, for example:

=== "Python"

    ``` python
    # create Xskipper instance for the sample dataset
    xskipper = Xskipper(spark, dataset_location)

    # remove index if exists
    if xskipper.isIndexed():
        xskipper.dropIndex()

    xskipper.indexBuilder() \
            .addMinMaxIndex("temp") \
            .addValueListIndex("city") \
            .addBloomFilterIndex("vid") \
            .build(reader) \
            .show(10, False)
    ```

=== "Scala"

    ``` scala
    // create Xskipper instance for the sample dataset
    val xskipper = new Xskipper(spark, dataset_location)

    // remove existing index if needed
    if (xskipper.isIndexed()) {
      xskipper.dropIndex()
    }

    xskipper.indexBuilder()
            .addMinMaxIndex("temp")
            .addValueListIndex("city")
            .addBloomFilterIndex("vid")
            .build(reader)
            .show(false)
    ```

By default, the indexes are stored as parquet files stored in storage  Each parquet file with row per each object in the dataset.  

For more information about the parquet metadatastore see [here](../../api/developer/parquet-metadatastore-spec/).

## Plugins

Xskipper supports adding new indexes using a [pluggable system](../../concepts/extensible/).  
For instructions on how to create a new plugin see [here](../creating-new-plugin/).

### Supported plugins

Currently the following plugins are supported (in addition to the built-in indexes: MinMax, ValueList and BloomFilter):

- [Regex Plugin](https://github.com/xskipper-io/xskipper-regex-plugin) - An index which enables to save a value list for a given regex.

### Setting up a plugin

In order to use a plugin you first need to register the needed classes.  
For example, for the [Regex Plugin](https://github.com/xskipper-io/xskipper-regex-plugin):

=== "Python"

    ``` python
    from xskipper import Registration
    
    Registration.addMetadataFilterFactory(spark, 'io.xskipper.plugins.regex.filter.RegexValueListMetaDataFilterFactory')
    # Add IndexFactory
    Registration.addIndexFactory(spark, 'io.xskipper.plugins.regex.index.RegexIndexFactory')
    # Add MetaDataTranslator
    Registration.addMetaDataTranslator(spark, 'io.xskipper.plugins.regex.parquet.RegexValueListMetaDataTranslator')
    # Add ClauseTranslator
    Registration.addClauseTranslator(spark, 'io.xskipper.plugins.regex.parquet.RegexValueListClauseTranslator')
    ```

=== "Scala"

    ``` scala
    import io.xskipper._
    import io.xskipper.plugins.regex.filter.RegexValueListMetaDataFilterFactory
    import io.xskipper.plugins.regex.index.RegexIndexFactory
    import io.xskipper.plugins.regex.parquet.{RegexValueListClauseTranslator, RegexValueListMetaDataTranslator}
    
    // registering the filter factories for user metadataFilters
    Registration.addIndexFactory(RegexIndexFactory)
    Registration.addMetadataFilterFactory(RegexValueListMetaDataFilterFactory)
    Registration.addClauseTranslator(RegexValueListClauseTranslator)
    Registration.addMetaDataTranslator(RegexValueListMetaDataTranslator)
    ```

### Index building

In order to build an index you can use the `addCustomIndex` API.   

For example for the [Regex Plugin](https://github.com/xskipper-io/xskipper-regex-plugin):

=== "Python"

    ``` python
        xskipper = Xskipper(spark, dataset_path)
        
        # adding the index using the custom index API
        xskipper.indexBuilder() \
            .addCustomIndex("io.xskipper.plugins.regex.index.RegexValueListIndex", ["log_line"],
                            {"io.xskipper.plugins.regex.pattern.r0": ".* .* .* (.*): .*"}) \
            .build(reader) \
            .show(10, False)
    ```

=== "Scala"

    ``` scala
    import io.xskipper.plugins.regex.implicits._
    
    // index the dataset
    val xskipper = new Xskipper(spark, dataset_path)


    xskipper
      .indexBuilder()
      // using the implicit method defined in the plugin implicits
      .addRegexValueListIndex("log_line", Seq(".* .* .* (.*): .*"))
      // equivalent
      //.addCustomIndex(RegexValueListIndex("log_line", Seq(".* .* .* (.*): .*")))
      .build(reader).show(false)

    ```

### Creating you own plugin

In order to create your own plugin see [here](api/creating-new-plugin/).
