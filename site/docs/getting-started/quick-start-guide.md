<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Quick-Start Guide

This guide helps you quickly get started using Xskipper with Apache Spark.

!!! note
    For advanced details see the [API](../api/indexing.md) section and the full [API Reference](../api/api-reference.md). 
    See [here](sample-notebooks.md) for sample notebooks.

## Setup Apache Spark with Xskipper

Xskipper is compatible with Apache Spark 3.x, There are two ways to setup Xskipper:

1. Run interactively: Start the Spark shell (Scala or Python) with Xskipper and explore Xskipper interactively.
2. Run as a project: Set up a Maven or SBT project (Scala or Java) with Xskipper.

### Run with an interactive shell

To use Xskipper interactively within the Spark’s Scala/Python shell, you need a local installation of Apache Spark.
Follow the instructions [here](https://spark.apache.org/downloads.html) to install Apache Spark.

#### Spark Scala Shell

Start a Spark Scala shell as follows:

```bash
./bin/spark-shell --packages io.xskipper:xskipper-core_2.12:{{extra_vars.version}}
```

#### PySpark

Install or upgrade PySpark (3.2 or above) by running the following:

```bash
pip install --upgrade pyspark
```

Then, run PySpark with the Xskipper package:

```bash
pyspark --packages io.xskipper:xskipper-core_2.12:{{extra_vars.version}}
```

### Run as a project

To build a project using the Xskipper binaries from the Maven Central Repository, use the following Maven coordinates:

#### Maven

Include Xskipper in a Maven project by adding it as a dependency in the project's POM file. Xskipper should be compiled with Scala 2.12.

```XML
<dependency>
  <groupId>io.xskipper</groupId>
  <artifactId>xskipper-core_2.12</artifactId>
  <version>{{extra_vars.version}}</version>
</dependency>
```

#### SBT
Include Xskipper in an SBT project by adding the following line to its build.sbt file:

```Scala
libraryDependencies += "io.xskipper" %% "xskipper-core" % "{{extra_vars.version}}"
```

#### Python

To set up a Python project, first start the Spark session using the Xskipper package and then import the Python APIs.

```Python
spark = pyspark.sql.SparkSession.builder.appName("Xskipper") \
    .config("spark.jars.packages", "io.xskipper:xskipper-core_2.12:{{extra_vars.version}}") \
    .getOrCreate()

from xskipper import Xskipper
from xskipper import Registration
```

## Configure Xskipper

In this example, we configure a JVM wide parameter to a base path which stores all data skipping indexes.
The indexes can be stored on the same storage system as the data, but not under the same path.
During query time indexes will be consulted at this location.

For more configuration options, see [configuration options](../api/configuration/configuration.md).

=== "Python"

    ``` python
    from xskipper import Xskipper

    # The base location to store all indexes 
    # TODO: change to your index base location
    md_base_location = "/tmp/metadata"

    # Configuring the JVM wide parameters
    conf = dict([
                ("io.xskipper.parquet.mdlocation", md_base_location),
                ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION"),
                ("io.xskipper.parquet.filter.dedup", "false")])
    Xskipper.setConf(spark, conf)
    ```

=== "Scala"

    ``` scala
    import io.xskipper._
    import io.xskipper.implicits._

    // The base location to store all indexes
    // TODO: change to your index base location
    val md_base_location = s"/tmp/metadata"

    // Configuring the JVM wide parameters
    val conf = Map(
      "io.xskipper.parquet.mdlocation" -> md_base_location,
      "io.xskipper.parquet.mdlocation.type" -> "EXPLICIT_BASE_PATH_LOCATION",
      "io.xskipper.parquet.filter.dedup" -> "false")
    Xskipper.setConf(conf)
    ```

## Indexing a Dataset

### Creating a Sample Dataset

First, let's create a sample dataset.

=== "Python"

    ``` python
    from pyspark.sql.types import *

    # TODO: change to your data location
    dataset_location = "/tmp/data"

    df_schema = StructType([StructField("dt", StringType(), True), StructField("temp", DoubleType(), True),\
                          StructField("city", StringType(), True), StructField("vid", StringType(), True)])

    data = [("2017-07-07", 20.0, "Tel-Aviv", "a"), ("2017-07-08", 30.0, "Jerusalem", "b")]

    df = spark.createDataFrame(data, schema=df_schema)

    # use partitionBy to make sure we have two objects
    df.write.partitionBy("dt").mode("overwrite").parquet(dataset_location)

    # read the dataset back from storage
    reader = spark.read.format("parquet")
    df = reader.load(dataset_location)
    df.show(10, False)
    ```

=== "Scala"

    ``` scala
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

    // TODO: change to your data location
    val dataset_location = s"/tmp/data"

    val schema = List(
      StructField("dt", StringType, true),
      StructField("temp", DoubleType, true),
      StructField("city", StringType, true),
      StructField("vid", StringType, true)
    )

    val data = Seq(
      Row("2017-07-07", 20.0, "Tel-Aviv", "a"),
      Row("2017-07-08", 30.0, "Jerusalem", "b")
    )

    val ds = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    // use partitionBy to make sure we have two objects
    ds.write.partitionBy("dt").mode("overwrite").parquet(dataset_location)

    // read the dataset back from storage
    val reader = spark.read.format("parquet")
    val df = reader.load(dataset_location)
    df.show(false)
    ```

### Indexing

When creating a data skipping index on a data set, first decide which columns to index, then choose an index type for each column.
These choices are workload and data dependent. Typically, choose columns to which predicates are applied in many queries.

The following index types are supported out of the box:

| Index type  | Description  | Applicable to predicates in WHERE clauses  | Column types |
|------------|--------------|--------------|--------------|
| MinMax |Stores minimum and maximum values for a column | <,<=,=,>=,> | All types except for complex types. See [Supported Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html). |
| ValueList | Stores the list of unique values for the column | =,IN,LIKE | All types except for complex types. See [Supported Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).|
| BloomFilter | Uses a bloom filter to test for set membership | =,IN | Byte, String, Long, Integer, Short |

MinMax results in small index size and is a good usually choice when the dataset's sort order is correlated with a given column. For the other 2 options, 
choose value list if the number of distinct values in an object is typically much smaller than the total number of values in that object.
On the other hand Bloom filters are recommended for columns with high cardinality (otherwise the index can get as big as that column of the data set).

Note that Xskipper also enables to create your own data skipping indexes and specify how to use them during query time. For more details see [here](../api/creating-new-plugin.md).

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

#### Viewing index status

The following code shows how a user can view the current index status to check which indexes exist on the dataset and whether the index is up-to-date.

=== "Python"

    ``` python
    xskipper.describeIndex(reader).show(10, False)
    ```

=== "Scala"

    ``` scala
    xskipper.describeIndex(reader).show(false)
    ```

#### List Indexed datasets

The following code shows how a user can view all indexed datasets under the current base location.

=== "Python"

    ``` python
    Xskipper.listIndexes(spark).show(10, False)
    ```

=== "Scala"

    ``` scala
    Xskipper.listIndexes(spark).show(false)
    ```

## Using Data Skipping Indexes

### Enable/Disable Xskipper

Xskipper provides APIs to enable or disable index usage with Spark.
By using the "enable" command, Xskipper optimization rules become visible to the Apache Spark optimizer and will be used in query optimization and execution.
By using the "disable' command, Xskipper optimization rules no longer apply during query optimization.
Note that disabling Xskipper has no impact on created indexes, and they remain intact.

=== "Python"

    ``` python
    # Enable Xskipper
    Xskipper.enable(spark)

    # Disable Xskipper
    Xskipper.disable(spark)

    # You can use the following to check whether the Xskipper is enabled
    if not Xskipper.isEnabled(spark):
        Xskipper.enable(spark)
    ```

=== "Scala"

    ``` scala
    // Enable Xskipper
    spark.enableXskipper()

    // Disable Xskipper
    spark.disableXskipper()

    // You can use the following to check whether the Xskipper is enabled
    if (!spark.isXskipperEnabled()) {
        spark.enableXskipper()
    }
    ```

### Running Queries

Once Xskipper has been enabled you can run queries (using either SQL or the DataFrame API) and enjoy the performance and cost benefits of data skipping. There will be no change to query results. 

First, let's create a temporary view:

=== "Python"

    ``` python
    df = reader.load(dataset_location)
    df.createOrReplaceTempView("sample")
    ```

=== "Scala"

    ``` scala
    df.createOrReplaceTempView("sample")
    ```

#### Example query using the MinMax index

=== "Python"

    ``` python
    spark.sql("select * from sample where temp < 30").show()
    ```

=== "Scala"

    ``` scala
    spark.sql("select * from sample where temp < 30").show()
    ```
<br/>
#### Inspecting query skipping stats
!!! Note
    Starting from version 1.3.0, skipping stats are disabled for queries involving
    Data Source V2 file sources. note that after processing a query that involves Data Source v2
    file sources, stats are disabled for all subsequent queries until the stats are cleared.
    See [this](https://github.com/xskipper-io/xskipper/issues/79) issue


=== "Python"

    ``` python
    Xskipper.getLatestQueryAggregatedStats(spark).show(10, False)
    ```

=== "Scala"

    ``` scala
    Xskipper.getLatestQueryAggregatedStats(spark).show(false)
    ```

Note: the above returns the accumulated data skipping statistics for all of the datasets which were involved in the query.
If you want to inspect the stats for a specific dataset you can call the API below to get stats on the Xskipper instance:

=== "Python"

    ``` python
    xskipper.getLatestQueryStats().show(10, False)
    ```

=== "Scala"

    ``` scala
    xskipper.getLatestQueryStats().show(false)
    ```

For more examples see the [sample notebooks](sample-notebooks.md)

## Index Life Cycle

The following operations can be used in order to maintain the index.

### Refresh Index

Over time the index can become stale as new files are added/removed/modified from the dataset.
In order to bring the index up-to-date you can call the refresh operation which will index the new/modified files and remove obsolete metadata.
Note: The index will still be beneficial for files which didn't change since the last indexing time even without refreshing.

First let's simulate addition of new data to the dataset:

=== "Python"

    ``` python
    # adding new file to the dataset to simulate changes in the dataset
    update_data = [("2017-07-09", 25.0, "Beer-Sheva", "c")]

    update_df = spark.createDataFrame(update_data, schema=df_schema)

    # append to the existing dataset
    update_df.write.partitionBy("dt").mode("append").parquet(dataset_location)
    ```

=== "Scala"

    ``` scala
    val update_data = Seq(
      Row("2017-07-09", 25.0, "Beer-Sheva", "c")
    )

    val update_ds = spark.createDataFrame(
      spark.sparkContext.parallelize(update_data),
      StructType(schema)
    )

    // append to the existing dataset
    update_ds.write.partitionBy("dt").mode("append").parquet(dataset_location)
    ```

Now, let's inspect the index status:

=== "Python"

    ``` python
    xskipper.describeIndex(reader).show(10, False)
    ```

=== "Scala"

    ``` scala
    xskipper.describeIndex(reader).show(false)
    ```

In this case the index status will indicate that there are new files that are not indexed. Therefore we use the Refresh operation to update the metadata:

=== "Python"

    ``` python
    xskipper.refreshIndex(reader).show(10, False)
    ```

=== "Scala"

    ``` scala
    xskipper.refreshIndex(reader).show(false)
    ```

Now you can run the `describe` operation again and see that the metadata is up to date.

### Drop Index

In order to drop the index use the following API call:

=== "Python"

    ``` python
    xskipper.dropIndex()
    ```

=== "Scala"

    ``` scala
    xskipper.dropIndex()
    ```

## Working with Hive tables

Xskipper also supports skipping over hive tables.  
Note that indexing is for Hive tables with partitions. To index tables without partitions, index the physical location directly.

The API for working with hive tables is similar to the API presented above with 2 main differences:

1. The uri used in the Xskipper constructor is the table identifier with the form: `<db>.<table>`.

2. The API calls do not require a DataFrameReader.

For more information regarding the API see [here](../api/indexing.md).

The index location for a hive table is resolved according to the following:

- If the table contains the parameter `io.xskipper.parquet.mdlocation` this value will be used as the index location.
- Otherwise, xskipper will look up the parameter `io.xskipper.parquet.mdlocation` in the table's database and will use it as the base index location for all tables.

Note: During indexing, the index location parameter can be automatically added to the table properties if the xskipper instance is configured accordingly.  
For more info regarding the index location configuration see [here](../api/configuration/parquet-mdstore-configuration.md#types-of-metadata-location).

### Setting the base index location in the database

In this example we will set the base location in the database.

=== "Python"

    ``` python
    alter_db_ddl = ("ALTER DATABASE default SET DBPROPERTIES ('io.xskipper.parquet.mdlocation'='{0}')").format(md_base_location)
    spark.sql(alter_db_ddl)
    ```

=== "Scala"

    ``` scala
    val alter_db_ddl = s"ALTER DATABASE default SET DBPROPERTIES ('io.xskipper.parquet.mdlocation'='${md_base_location}')"
    spark.sql(alter_db_ddl)
    ```

#### Creating a Sample Hive Table

Let's create a hive table on the dataset we created earlier:

=== "Python"

    ``` python
    create_table_ddl = """CREATE TABLE IF NOT EXISTS tbl ( \
    temp Double,
    city String,
    vid String,
    dt String
    )
    USING PARQUET
    PARTITIONED BY (dt)
    LOCATION '{0}'""".format(dataset_location)
    spark.sql(create_table_ddl)

    # recover the partitions
    spark.sql("ALTER TABLE tbl RECOVER PARTITIONS")

    # verify the table was created
    spark.sql("show tables").show(10, False)
    spark.sql("show partitions tbl").show(10, False)
    ```

=== "Scala"

    ``` scala
    val create_table_ddl =
          s"""CREATE TABLE IF NOT EXISTS tbl (
             |temp Double,
             |city String,
             |vid String,
             |dt String
             |)
             |USING PARQUET
             |PARTITIONED BY (dt)
             |LOCATION '${dataset_location}'
             |""".stripMargin
    spark.sql(create_table_ddl)

    // Recover the table partitions
    spark.sql("ALTER TABLE tbl RECOVER PARTITIONS")

    // verify the table was created
    spark.sql("show tables").show(false)
    spark.sql("show partitions tbl").show(false)
    ```

### Indexing a Hive Table

Note we use default.sample as the uri in the Xskipper constructor.

=== "Python"

    ``` python
    # create an Xskipper instance for the sample Hive Table
    xskipper_hive = Xskipper(spark, 'default.tbl')

    # remove index if exists
    if xskipper_hive.isIndexed():
        xskipper_hive.dropIndex()

    xskipper_hive.indexBuilder() \
            .addMinMaxIndex("temp") \
            .addValueListIndex("city") \
            .addBloomFilterIndex("vid") \
            .build() \
            .show(10, False)
    ```

=== "Scala"

    ``` scala
    // create an Xskipper instance for the sample Hive Table
    val xskipper_hive = new Xskipper(spark, "default.tbl")

    // remove existing index if needed
    if (xskipper_hive.isIndexed()) {
      xskipper_hive.dropIndex()
    }

    xskipper_hive.indexBuilder()
            .addMinMaxIndex("temp")
            .addValueListIndex("city")
            .addBloomFilterIndex("vid")
            .build()
            .show(false)
    ```

### Running Queries

Once Xskipper has been enabled you can continue running queries (using either SQL or DataFrame API) and enjoy the benefits of data skipping.

First, let's make sure Xskipper is enabled:

=== "Python"

    ``` python
    # You can use the following to check whether the Xskipper is enabled
    if not Xskipper.isEnabled(spark):
        Xskipper.enable(spark)
    ```

=== "Scala"

    ``` scala
    // You can use the following to check whether the Xskipper is enabled
    if (!spark.isXskipperEnabled()) {
        spark.enableXskipper()
    }
    ```

#### Example query using the MinMax index

=== "Python"

    ``` python
    spark.sql("select * from tbl where temp < 30").show(false)
    ```

=== "Scala"

    ``` scala
    spark.sql("select * from tbl where temp < 30").show(false)
    ```

Inspecting the query stats:

=== "Python"

    ``` python
    Xskipper.getLatestQueryAggregatedStats(spark).show(10, False)
    ```

=== "Scala"

    ``` scala
    Xskipper.getLatestQueryAggregatedStats(spark).show(false)
    ```

### Index Life Cycle - Hive Tables

The API is similar to the dataset API but without the need for a `reader` instance.

#### View the index status

=== "Python"

    ``` python
    xskipper_hive.describeIndex().show(10, False)
    ```

=== "Scala"

    ``` scala
    xskipper_hive.describeIndex().show(false)
    ```

#### Refresh Index

=== "Python"

    ``` python
    xskipper_hive.refreshIndex().show(10, False)
    ```

=== "Scala"

    ``` scala
    xskipper.refreshIndex(reader).show(false)
    ```

#### Drop Index

In order to drop the index use the following API call:

=== "Python"

    ``` python
    xskipper_hive.dropIndex()
    ```

=== "Scala"

    ``` scala
    xskipper_hive.dropIndex()
    ```
