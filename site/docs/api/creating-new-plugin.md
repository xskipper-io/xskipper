<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Using The Extensible API

## Intro

!!! note
    This page explains the Extensible Data Skipping framework API.   
    See the [Concepts](../concepts/data-skipping.md) page for an explanation about the underlying concepts.   
    See [here](indexing.md#supported-plugins) for the list of currently available plugins.
    
Xskipper supports adding your index types and specifying your own data skipping logic
in order to enjoy data skipping over UDFs and a variety of data types.

The pluggability is split between two main areas:

- [Indexing Flow](#indexing-flow)
    * Interfaces for defining a new index - implementations of [Index](../scaladoc/{{extra_vars.version}}/io/xskipper/index/index.html) along with [IndexFactory](../scaladoc/{{extra_vars.version}}/io/xskipper/index/IndexFactory.html).
    * Interface for specifying how to store the metadata in the metadatastore - implementations of [MetaDataTranslator](../scaladoc/{{extra_vars.version}}/io/xskipper/metadatastore/MetaDataTranslator.html).

- [Query Evaluation Flow](#query-evaluation-flow) 
    * Interfaces for defining how query expressions are mapped to abstract conditions on metadata - implementations of [MetaDataFilter](../scaladoc/{{extra_vars.version}}/io/xskipper/search/filters/MetadataFilter.html) along with [MetaDataFilterFactory](../scaladoc/{{extra_vars.version}}/io/xskipper/search/filters/MetadataFilterFactory.html).
    * Interfaces for translating an abstract clause to a specific implementation according to the metadatastore - Implementations of [ClauseTranslator](../scaladoc/{{extra_vars.version}}/io/xskipper/metadatastore/ClauseTranslator.html).
    
A new plugin can contain one or more of the above implementations.
 
This architecture enables the registration of components using multiple packages.

## Using Existing Plugins

To use a plugin, load the relevant implementations using the Registration module ([Scala](../scaladoc/{{extra_vars.version}}/io/xskipper/Registration$.html), [Python](../pythondoc/{{extra_vars.version}}/index.html#module-xskipper.registration)).   
For example:

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

For the full list of plugins see [here](indexing.md#supported-plugins).

<br/>

!!! note
    When registering multiple plugins the order of registration for `IndexFactory`, `MetadataTranslator`, and `ClauseTranslator` matters.
    If two plugins define relevant translations or index creation for the same parameters the first one registered will be used.   
    In general, you should avoid having multiple plugins that behave differently for the same indexes, metadata types or clauses.


In the following sections we explain how to use the above interfaces in order to create a new plugin.   
The explanations will use examples from the sample plugin - [xskipper-regex-plugin](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/index/RegexValueListIndex.scala).

The regex plugin enables indexing a text column by specifying a list of patterns and saving the matching substrings as a value list.

For example, consider an application log dataset and one of its objects :
```csv
application_name,log_line
batch job,20/12/29 18:04:39 INFO FileSourceStrategy: Pruning directories with:
batch job,20/12/29 18:04:40 INFO DAGScheduler: ResultStage 22 (collect at ParquetMetadataHandle.scala:324) finished in 0.011 s
```

and the regex pattern `".* .* .* (.*): .*"`.

When we index using the regex index, the metadata that will be saved is `List("FileSourceStrategy", "DAGScheduler")`.

The following query will benefit from this index and will skip the above object:
```SQL
SELECT * 
FROM tbl 
WHERE 
regexp_extract(log_line, '.* .* .* (.*): .*', 1) = 'MemoryStore'
```

## Indexing Flow

### Define the abstract metadata

*Implementation(s) of [MetaDataType](../scaladoc/{{extra_vars.version}}/io/xskipper/index/metadata/MetadataType.html)*

First you need to define the abstract metadata type that will be generated by the index. This type will hold the metadata in memory.   
For example, the MinMax index metadata type is a tuple of min and max values (see [here](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/index/metadata/MinMaxMetaData.scala))

For the Regex plugin, to store the unique list of matching substrings for a given pattern we use a HashSet of Strings (see [here](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/index/RegexValueListMetadata.scala)).

### Define a new index

*Implementation(s) of [Index](../scaladoc/{{extra_vars.version}}/io/xskipper/index/index.html) along with [IndexFactory](../scaladoc/{{extra_vars.version}}/io/xskipper/index/IndexFactory.html)*

Support for new indexes can be achieved by implementing a new class that implements the [Index](../scaladoc/{{extra_vars.version}}/io/xskipper/index/index.html) abstract class.  
A new index can use an existing MetaDataType or create its own MetaDataType along with a translation specification to the relevant metadatastore.

The Index interface enables specifying one of two ways to collect the metadata:

- Tree Reduce - in this code path the index processes the object row by row and updates its internal state to reflect the update to the metadata.
This mode enables running index creation in parallel for multiple indexes.

- Optimized - using this interface the index processes the entire object DataFrame and generates the metadata.

For example, both the [MinMaxIndex](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/index/MinMaxIndex.scala) and the [RegexValueListIndex](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/index/RegexValueListIndex.scala).
use the Tree Reduce mode to accumlate the list of unique matches.

Along with the Index you need to define an [IndexFactory](../scaladoc/{{extra_vars.version}}/io/xskipper/index/IndexFactory.html) - 
the IndexFactory specifies how to recreate the index instance when loading the index parameters from the metadatastore. 
For example, see [RegexIndexFactory](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/index/RegexIndexFactory.scala).

### Define translation for the metadata

*Implementation(s) of [MetaDataTranslator](../scaladoc/{{extra_vars.version}}/io/xskipper/metadatastore/MetaDataTranslator.html)*

!!! info
    Xskipper uses by default Parquet as the metadatastore.    
    The Parquet metadatastore stores the metadata for the objects as rows in parquet files (for more details see [here](../developer/parquet-metadatastore-spec/)).      
    The API enables defining your own metadatastore. Here we focus on storing the metadata in the Parquet metadatastore. Therefore, the translations we cover here relate to the Parquet metadatastore.
    

In order to store the abstract metadata defined above in the metadata store we have to specify a suitable translation which will map it to a valid representation for the metadatastore.

For the Parquet metadatastore we have two options:

- Convert the metadata to an internal Spark [Row](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Row.html) which will later be saved to Parquet automatically, as Spark supports Parquet out of the box.
For example, the MinMax index translates its values to a nested row with `min` and `max` values (see [here](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/metadatastore/parquet/ParquetBaseClauseTranslator.scala#L28))

- Use a UDT to save the serialized abstract metadata. In some cases translating the metadata to Spark Row is not possible, therefore we save the metadata as a serialized binary.    
To do so you have 2 options:

    * Use the default java serialization provided by the parquet metadata store. To do so you need to define the UDT and register it. For example, for bloom filter, we use the following definition to get the default java serialization:
    
    ```Scala
    class BloomFilterMetaDataTypeUDT extends MetadataTypeUDT[BloomFilterMetaData]
    ```
  
    Then register the UDT to Spark using the [ParquetMetadataStoreUDTRegistration](../scaladoc/{{extra_vars.version}}/org/apache/spark/sql/types/ParquetMetadataStoreUDTRegistration$.html) object:
    ```Scala
    ParquetMetadataStoreUDTRegistration.registerUDT(classOf[BloomFilterMetaData].getName, classOf[BloomFilterMetaDataTypeUDT].getName)
    ```
  
    Note that the bloom filter has the above definition built in so there is no need to register it.
  
    !!! note
        The UDT must be defined and registered in any program that uses the index.   
        A recommended pattern is to define and register the UDT in the [Clause Translator](#define-translation-for-the-clause) object where you also define the Clause Translation logic.    
        This object will be loaded when [registering](#using-existing-plugins) the Clause Translator.
    
    * Define your own UDT with custom serialization logic - similar to the above only this time you implement your own UDT.    
    See the [`MetadataTypeUDT`](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/metadatastore/parquet/ParquetMetadataTypeUDT.scala) class for a reference.

For the regex plugin we use the first option and translate the list of values to an array of values for storing in Parquet format (see [here](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/parquet/RegexValueListMetaDataTranslator.scala)).

## Query Evaluation Flow

### Define the abstract clause

*Implementations of [Clause](../scaladoc/{{extra_vars.version}}/io/xskipper/search/clause/Clause.html)*

First, you need to define the abstract clause that will be created by the Filter.    
The Clause specifices an abstract condition which was deduced from the query and should operate on the metadata in order to determine the relevant objects.
Each Clause is then translated to an explicit implementation according to the metadatastore type.   

For example, for the MinMax index we define a [MinMaxClause](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/search/clause/MinMaxClause.scala) which follows the logic that was presented [here](../concepts/query-evaluation-flow.md#clause).

For the Regex Plugin we use a Clause which holds the required matching patterns from the query (see [here](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/clause/RegexValueListClause.scala)).

### Define a new filter

*Implementation(s) of [MetaDataFilter](../scaladoc/{{extra_vars.version}}/io/xskipper/search/filters/MetadataFilter.html) along with [MetaDataFilterFactory](../scaladoc/{{extra_vars.version}}/io/xskipper/search/filters/MetadataFilterFactory.html)*

The filter processes the query tree and labels it with clauses. In most cases we would like to map expressions to clauses.
Therefore, xskipper provides a basic implementation of a filter called [BaseMetadataFilter](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/search/filters/MetadataFilter.scala)
which processes the query tree automatically for AND and OR operators, leaving the user to handle only the remaining expressions. Implementations which extend the [BaseMetadataFilter](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/search/filters/MetadataFilter.scala) need only specify how expressions are mapped to clauses.   
For example,  [RegexValueListFilter](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/filter/RegexValueListFilter.scala) and [MinMaxFilter](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/search/filters/MinMaxFilter.scala) map the query expressions according to the logic presented [here](../concepts/query-evaluation-flow.md#filter).

A more advanced filter can process the entire tree by implementing the `MetaDataFilter` class without using the `BaseMetadataFilter`.

Along with a Filter you need to define a [MetadataFilterFactory](../scaladoc/{{extra_vars.version}}/io/xskipper/search/filters/MetadataFilterFactory.html). The MetadataFilterFactory specifies which filters should run given the available indexes.
For example, see the [RegexIndexFactory](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/index/RegexIndexFactory.scala).

### Define translation for the clause

*Implementations of [ClauseTranslator](../scaladoc/{{extra_vars.version}}/io/xskipper/metadatastore/ClauseTranslator.html)*

!!! info
    Xskipper uses Parquet as the metadatastore by default.    
    The Parquet metadatastore stores the metadata for the objects as rows in parquet files (for more details see [here](developer/parquet-metadatastore-spec.md)).    
    Spark is used as the engine to run the abstract clauses on the metadata.     
    The API enables defining your own metadatastore. Here we focus on the metadata in the Parquet metadatastore. Therefore, the translations are relevant to the Parquet metadatastore.
    

In order to process a clause, the abstract clause defined above needs to be translated to a form that is executable by the metadatastore.

For the Parquet metadatastore we have 2 options:

- Translate the Clause to a native Spark operation - this is useful when you have a built-in expression in Spark that can process the metadata. For example, for the MinMax index we use Spark’s built-in inequality operators (>, <, >=, <=) to translate the abstract clause (see [here](https://github.com/xskipper-io/xskipper/blob/0b430e9df3abaec2ee8ec530c702b6534c9f751f/src/main/scala/io/xskipper/metadatastore/parquet/ParquetBaseClauseTranslator.scala#L56)).
    
- Use a UDF that will process the metadata - this is useful when the metadata is saved by serializing the abstract metadata type 
or when there is no built-in operation that implements the logic needed in order to process the metadata.     
For example, for the BloomFilter index which we serializes its metadata, we use a UDF to check whether the given value exists in the metadata or not (see [here](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/metadatastore/parquet/ParquetBaseClauseTranslator.scala)).

For the Regex Plugin we translate the clause to use Spark's `arrays_overlap` and `array_except` functions in order to check if the values in the clause exist in the metadata (see [here](https://github.com/xskipper-io/xskipper-regex-plugin/blob/master/src/main/scala/io/xskipper/plugins/regex/parquet/RegexValueListClauseTranslator.scala)).
