<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# Parquet Metadatastore properties

Xskipper uses Parquet as the metadatastore.  
The Parquet metadatastore store the metadata for the objects as rows in parquet file. See [here](../developer/parquet-metadatastore-spec.md) for more details.

The following are parameters relevant for the Parquet metadatastore.  

These parameters can be set as:

- JVM wide configuration as described [here](configuration.md#setting-jvm-wide-configuration).
- Specific Xskipper instance as described [here](configuration.md#setting-configuration-for-a-specific-xskipper-instance).

| Property   | Default  | Description |
|------------|--------------|--------------|
|io.xskipper.parquet<br/>.mdlocation | N/A | The metadata location according to the type|
|io.xskipper.parquet<br/>.mdlocation.type |	EXPLICIT_BASE_PATH_LOCATION | See [here](#types-of-metadata-location)|
|io.xskipper.parquet<br/>.index.chunksize | 25000 | The number of objects to index in each chunk |
|io.xskipper.parquet<br/>.maxRecordsPerMetadataFile | 50000 | The number of records per metadata file used in the compact stage to limit the number of records for small sized metadata files. |
|io.xskipper.parquet<br/>.maxMetadataFileSize | 33554432 (32MB) | The expected max size of each metadata file will be used by the compact function to distribute the data to multiple files to distribute the data to multiple files in such way that each is less than the expected max size of each metadata file used in conjunction with the max records per file configuration |
|io.xskipper.parquet<br/>.encryption.plaintext.footer | false | Whether or not to use plain footer |
|io.xskipper.parquet<br/>.encryption.footer.key | N/A | The encryption key that will be used to encrypt the metadata footer |
|io.xskipper.parquet<br/>.refresh.dedup | true | When set to true each refresh operation will drop duplicates metadata entries (which might exist due to failed refresh operations) |
|io.xskipper.parquet<br/>.filter.dedup | true | When set to true duplicate metadata entries (which might exist due to failed refresh operations) will be dropped when filtering in query run time |

!!! Note
Deduplication during filtering is documented as `false` by default, but in version `1.2.3` it accidentally defaults to `true`.
this cannot affect query results, but may degrade performance.
as a workaround, it can be manually set `io.xskipper.parquet.filter.dedup` to `false`.

## Types of metadata location

The parquet metadatastore looks up the metadata location according to the paramater `io.xskipper.parquet.mdlocation`, this parameter  is interpreted according to the URL type defined in the parameter `io.xskipper.parquet.mdlocation.type`.  

The following options are available for the parameter `io.xskipper.parquet.mdlocation.type`:

- `EXPLICIT_BASE_PATH_LOCATION`: This is the default. An explicit definition of the base path to the metadata, which is combined with a data set identifier. This case can be used to configure the Xskipper JVM wide settings and have all of data sets metadata saved under the base path.
- `EXPLICIT_LOCATION`: An explicit full path to the metadata.
- `HIVE_TABLE_NAME`: The name of the Hive table (in the form of `<db>.<table>`) that contains the exact path of the metadata in the table properties under the parameter `io.xskipper.parquet.mdlocation`.
- `HIVE_DB_NAME`: The name of the Hive database that contains the base path of the metadata in the database properties under the parameter `io.xskipper.parquet.mdlocation`.

For more information on how the metadata path is resolved see [here](#metadata-path-resolving).

You should set the `io.xskipper.parquet.mdlocation` in one of two ways:

### Setting base location for metadata (recommended)

  This configuration is useful for setting base location once for all datasets and should be used with the `EXPLICIT_BASE_PATH_LOCATION` or `HIVE_DB_NAME` types.

  The location of the metadata for each data set will be inferred automatically by combining the base path with a data set identifier.
  
  For example, setting `EXPLICIT_BASE_PATH_LOCATION`:

=== "Python"

    ``` python
    from xskipper import Xskipper
    
    # The base location to store all indexes 
    # TODO: change to your index base location
    md_base_location = "/tmp/metadata"
    
    # Configuring the JVM wide parameters
    conf = dict([
                ("io.xskipper.parquet.mdlocation", md_base_location),
                ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
    Xskipper.setConf(spark, conf)
    ```

=== "Scala"

    ``` scala
    from xskipper import Xskipper
    
    # The base location to store all indexes 
    # TODO: change to your index base location
    md_base_location = "/tmp/metadata"
    
    # Configuring the JVM wide parameters
    conf = dict([
                ("io.xskipper.parquet.mdlocation", md_base_location),
                ("io.xskipper.parquet.mdlocation.type", "EXPLICIT_BASE_PATH_LOCATION")])
    Xskipper.setConf(spark, conf)
    ```

### Setting an explicit metadata location for Xskipper instance

  This configuration is useful for setting a specific metadata location for a certain data set and should be used with the `EXPLICIT_LOCATION` or `HIVE_TABLE_NAME` type.  
  You can also set it to `EXPLICIT_BASE_PATH_LOCATION` or `HIVE_DB_NAME` if you want to override the JVM defaults. 
  
  For example setting an `EXPLICIT_LOCATION` for xskipper instance:

=== "Python"

    ``` python
    xskipper = Xskipper(spark_session, dataset_location)
    
    # call set params to make sure it overwrites JVM wide config
    params = dict([
        ('io.xskipper.parquet.mdlocation', '<your metadata>'),
        ('io.xskipper.parquet.mdlocation.type', 'EXPLICIT_BASE_PATH_LOCATION')])
    xskipper.setParams(params)
    ```

=== "Scala"

    ``` scala
    val xskipper = new Xskipper(spark, dataset_location)
    
    // call set params to make sure it overwrites JVM wide config
    val params = Map(
      "io.xskipper.parquet.mdlocation" -> "<your metadata>",
      "io.xskipper.parquet.mdlocation.type" ->  "EXPLICIT_BASE_PATH_LOCATION")
    xskipper.setParams(params)
    ```
  <br/>
  Note that when setting the type to `HIVE_TABLE_NAME` you should first set the parameter `io.xskipper.parquet.mdlocation` in the table properties to point to the metadata location.  
  
  You can the set the metadata location for hive table manually using the following DDL:
    ```sql
    ALTER TABLE myDb.myTable
    SET TBLPROPERTIES ('io.xskipper.parquet.mdlocation'='/location/of/metadata')
    ```
  
  If the location does not exist in the Hive table, xskipper will try to look up the parameter `io.xskipper.parquet.mdlocation` in the table's database and treat it as a base path location.
 
  When using `HIVE_TABLE_NAME` as the location type once indexing is done the parameter `io.xskipper.parquet.mdlocation` is set in the table properties.  
  (in case a fallback to the database base path occured then the resolved path from using the database metadata location as base path is stored in the table properties)

### Metadata path resolving

The following explains how the metadata path is resolved:

 - During Indexing the metadata location is inferred according to the parameters parameters `io.xskipper.parquet.mdlocation` and `io.xskipper.parquet.mdlocation.type` which are set on the xskipper instance.  
These parameters are the default JVM wide parameters unless set differently for the given instance.

- During query run time, the metadata location is inferred according to the following

    * If there is an active xskipper instance in the JVM for the dataset/table use it's metadata configuration. Otherwise: 

    * For Datasets:
        * Look up the parameters `io.xskipper.parquet.mdlocation` and `io.xskipper.parquet.mdlocation.type` in the JVM wide configuration in order to infer the metadata location.

    * For Hive tables (with partitions[^1]):
        * If the table contains the parameter `io.xskipper.parquet.mdlocation` use it as the metadata location.
        * If not, look up the parameter `io.xskipper.parquet.mdlocation` in the table's database and treat it as a base path location.

[^1]: To index tables without partitions, index the physical location directly.