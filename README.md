# Delta_Log_History_and_Unpacking_Struct_DataTypes

### Extracting Data from delta_log Files to Report on Schema Evolution

The data displayed in this repository is based on results from running notebooks 1-3 located in my related repository [Databricks_Schema_Evolution_With_Autoloader](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader).

You may also execute the following steps with your own delta table data with some simple simple modifications of the relevant folder or file paths in your personal or corporate enviroment. 

In the coming steps we will:
1. Explore our delta_log files
2. Open a delta log file to explore the metaData and CommitInfo
3. Extract, join, and analyze the data to create a human readable record of the schema evolution of our bronze layer table

## What is a delta_log File?

Delta log or transaction log files are a record of changes that occur on a delta table. 

More concretely, each delta log file represents an atomic change that takes place on the underlying Parquet files that make up the delta table. As Parquet files are inherently immutable, meaning that the data in the file cannot be edited in place, the delta log simulates changes to the underlying parquet by replacing versions of that parquet file with new versions each time the delta table is updated. 

For our purposes we will be exploring meta data on schema that stored in the delta_log files.

You can read more on delta_log files at databricks [official article](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html).


## Querying to see the delta_log files in Your FileStore

We can use the % fs ls followed by our Databricks FileStore path to see the log files we created in ingesting data into our bronze layer table bronze_tj_fact_revenue. 

![image](https://github.com/user-attachments/assets/4bf4b792-b072-4721-b5df-e774122e1d27)

## Opening a delta_log File

Delta log files are stored in the JSON file format. We can open them by using the .read and .json methods. In this example I am opening the 6th log file with the path "/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue/_delta_log/00000000000000000006.json". You could replace this with your relevant file path if following along with a different data set. 

```python
json1 = spark.read.json("/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue/_delta_log/00000000000000000006.json")

display(json1)
```

Running the above shows the underlying underlying data in the JSON. Each delta_log file is an assortment of JSON dictionary objects known as Structs as seen below. Each delta log file can contain different structs such as Add, commitInfo, and metaData. 

![image](https://github.com/user-attachments/assets/8c02c1d6-efdd-48ad-82ed-9734fc6e18be)

We are most interested in the timestamp field and SchemaString located in the commitInfo and metaData structs. 

## Extracting Values from the delta_log metaData and commitInfo Structs

Before extraction of the desired values from the structs we must first perform some enrichment and transformations on our data. At an enterprise scale, we would want to include a functional module or notebook of this code as part of a pipeline that would write our schema evolution metadata to a central location so that we could monitor schema drift across our bronze layer tables to monitor these changes with our data producers and business stakeholders. 

In order to create an insertable record that would serve this purpose, we add columns for the table name and schema version, as well as self join to allign the schemaInfo and metaData for the same schema version in the same row. 

``` Python
from pyspark.sql.functions import input_file_name, regexp_extract

spark.read.json("dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue/_delta_log/*.json")\
    .withColumn("table_name", regexp_extract(input_file_name(), r'/tables/TraderJoesRevenue/(.*?)/_delta_log/', 1))\
    .withColumn("schema_version", regexp_extract(input_file_name(), r'(\d+)\.json$', 0))\
    .createOrReplaceTempView("delta_log")

display(spark.sql(" \
    select a.table_name, a.schema_version, a.metaData, b.commitInfo \
    from delta_log a \
    left join delta_log b on a.schema_version = b.schema_version AND b.commitInfo is not null\
    WHERE a.metaData is not null and a.schema_version <> '' \
    ORDER BY a.schema_version DESC"))
```

![image](https://github.com/user-attachments/assets/20e1d828-daec-479e-ae87-dac5c1e2a195)

Next we want to use a SQL lag function in conjunction with extracting the schema string from the metaData struct to bring the previous schema and current schema on the same line. 

``` a.metaData.schemaString AS current_schemaString, LAG(a.metaData.schemaString, 1) OVER(PARTITION BY a.table_name ORDER BY a.schema_version) AS prev_schemaString ```

In the same step we also extract the timestamp from the commitInfo Struct by converting the value from unixtime to something human readable. Note that we retain the original structs 

``` from_unixtime(b.commitInfo.timestamp / 1000) as timestamp```

```Python
# Struct Selection to access schema variances using windowing
from pyspark.sql.functions import input_file_name, regexp_extract, col, explode, struct, from_unixtime

spark.read.json("dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue/_delta_log/*.json")\
    .withColumn("table_name", regexp_extract(input_file_name(), r'/tables/TraderJoesRevenue/(.*?)/_delta_log/', 1))\
    .withColumn("schema_version", regexp_extract(input_file_name(), r'(\d+)\.json$', 0))\
    .createOrReplaceTempView("delta_log")

#windowing function here
df =(spark.sql(" \
    select a.table_name, a.schema_version, from_unixtime(b.commitInfo.timestamp / 1000) as timestamp, \
    a.metaData.schemaString AS current_schemaString, LAG(a.metaData.schemaString, 1) OVER(PARTITION BY a.table_name ORDER BY a.schema_version) AS prev_schemaString ,b.commitInfo , a.metaData  \
    from delta_log a \
    left join delta_log b on a.schema_version = b.schema_version AND b.commitInfo is not null\
    WHERE a.metaData is not null and a.schema_version <> '' \
    ORDER BY a.schema_version DESC"))

display(df)
```
![image](https://github.com/user-attachments/assets/b393039f-2ac2-4724-b52d-ca4dd92032ba)

## Writing a Function to Compare the Previous and Current Schemas and Extract the Changes

In the below code we define a function to work with the schema strings and compare them to one another. The function works by creating a set for each of the struct objects (current_schema.fields and prev_schema.fields) then comparing the Current and Previous sets to each other 

```Python
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType

display(df)

# write a UDF
def compare_schemas(current_schema, prev_schema):
    if prev_schema is None:
        return 'No previous schema'
        # Directly work with StructType's fields in two set operations to compare the two schemas current_schema.fields and prev_schema.fields
    current_fields = set(field.name for field in current_schema.fields) if current_schema else set() 
        
    prev_fields = set(field.name for field in prev_schema.fields) if prev_schema else set() 
        
    added = current_fields - prev_fields
    removed = prev_fields - current_fields
    return f"Added: {list(added)}, Removed: {list(removed)}"

# Converting the output to stringtype (the code doesn't work without this which I don't completly understand TBH)
compare_schemas_udf = udf(compare_schemas, StringType())

# Apply UDF to the Date Frame earlier
df = df.withColumn("schema_changes", compare_schemas_udf(from_json(col("current_schemaString"), schema), from_json(col("prev_schemaString"), schema)))

# Write a temp view again
df.createOrReplaceTempView("delta_log_enhanced")

# Display the result and reorder columns with SparkSQL
display(spark.sql("""
    SELECT table_name, schema_version, timestamp, schema_changes, current_schemaString, prev_schemaString, metaData, commitInfo
    FROM delta_log_enhanced
    ORDER BY schema_version DESC
"""))
```
### The end result is a record, in this case group of records that can be inserted into a traditional database or maintained in the delta format and used for stakeholder reporting on questions such as:
1. How frequently is upstream data changing?
2. Which bronze layer tables are effected?
3. When did the schema change?
4. What were the schema changes?
![image](https://github.com/user-attachments/assets/91dc6469-65b9-44b8-900e-de928653aaf5)

