<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Configuration

Xskipper enables to configure the instances using JVM wide configtation which applies for all created Xskipper instances.  
In addition one can set specific configuration for a given instance as described below.

For the configuration related to the parquet metadatastore see [here](/api/parquet-mdstore-configuration).

## Setting JVM wide configuration

Use the following to set JVM wide configuration properties for Xskipper instances.  
These configurations will be applied by default on each new instance.

=== "Python"

    ``` python
    from xskipper import Xskipper
    # TODO: fill with config values
    conf = dict()
    Xskipper.setConf(spark, conf)
    ```

=== "Scala"

    ``` scala
    import io.xskipper._
    // TODO: fill with config values
    val conf = Map()
    Xskipper.setConf(conf)
    ```

## Setting configuration for a specific Xskipper instance

Use the following to set the configuration for a specific Xskipper instance

=== "Python"

    ``` python
    # call set params to make sure it overwrites JVM wide config
    # TODO: fill with config values
    params = dict()
    xskipper.setParams(params)
    ```

=== "Scala"

    ``` scala
    val xskipper = new Xskipper(spark, dataset_location)
    
    // call set params to make sure it overwrites JVM wide config
    // TODO: fill with config values
    val params = Map()
    xskipper.setParams(params)
    ```

## Xskipper properties

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| io.xskipper.evaluation.enabled| false | When true, queries will run in evaluation mode.<br/>When running in evaluation mode all of the indexed dataset will only be processed for skipping stats and no data will be read. The evaluation mode is useful when we want to inspect the skipping stats  |
| io.xskipper.timeout | 10 | The timeout in minutes to retrieve the indexed objects and the relevant objects for the query from the metadatastore |
| io.xskipper.identifierclass | io.xskipper.utils.<br/>identifier.Identifier | The fully qualified name of an identifier class to be loaded using reflection used to specify how the table identifier and file ID are determined. For more information see [here](#identifier-class).
| io.xskipper.index.parallelism | 10 | defines the number of concurrent objects that will be indexed
| io.xskipper.index.minchunksize | 1 | Defines the minimum chunk size to be used when indexing. <br/> The chunk size will be multiplied by 2 till reaching the metadataStore upload chunk size
| io.xskipper.index.bloom.fpp | 0.01 | Indicates the bloom filter default fpp
| io.xskipper.index.bloom.ndv | 100000 | Indicates the bloom filter expected number of distinct values
| io.xskipper.index.minmax<br/>.readoptimized.parquet | true | Indicates whether the collection of min/max stats for Numeric columns when working with Parquet objects will be done in an optimized way by reading the stats from the Parquet footer
| io.xskipper.index.minmax<br/>.readoptimized.parquet.parallelism | 10000 | The number of objects to be indexed in parallel when having only minmax indexes on parquet objects
| io.xskipper.index.minmax<br/>.inFilterThreshold | 100 | defines number of values in an IN filter above we will push down only one min/max condition based on the min and maximum of the entire list on parquet objects
| io.xskipper.index.memoryFraction | 0.2 | The memory fraction from the driver memory that indexing will use in order to determine the maximum chunk size dynamically

## Identifier Class

Each dataset/table/file that that is indexed by Xskipper is identified by some Identifier.
This identifier is used for in order to determine the following:

- The file ID for each indexed file - typically the file ID is comprised of the file name concatenated with the last modified time stamp to detect changes since last indexing time.
- Display name for identifiers
- Disply name for paths

Currently Xskipper supports two implementation of the Identifer class:

- [Default Identifier](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/utils/identifier/Identifier.scala)
    * The File ID is  - `<file_name>#<last_modification_time>`
    * The Identifier is the uri after removing a trailing `/` if exists.
    * The Path and Display name are the given paths

- [IBM COS Identifier](https://github.ibm.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/utils/identifier/IBMCOSIdentifier.scala) - differs from the default identifier by
    * The identifier for IBM COS paths when using [Stocator](https://github.com/CODAIT/stocator) is removing the `service` part and keeps the bucket name.  
    So, for example the identifier for `cos://mybucket.service/path/to/dataset/` is `cos://mybucket/path/to/dataset` 

For most cases you can use the default implementation, however if you require some custom logic you can add your own implementation for the [Identifier](https://github.com/xskipper-io/xskipper/blob/master/src/main/scala/io/xskipper/utils/identifier/Identifier.scala) class (for example, the IBM COS Identifier as an example).  
The Identifier class is by setting the paramater `io.xskipper.identifierclass` to the and determines the logic for inferring the identifier for each 
 