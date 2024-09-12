# Delta_Log_History_and_Unpacking_JSON_Dict_Databricks

### Extracting Data from delta_log files to Report on Schema Evolution

The data displayed in this repository is based on results from running notebooks 1-3 located in my related repository [Databricks_Schema_Evolution_With_Autoloader](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader).

You may also execute the following steps with your own delta table data with some simple simple modifications of the relevant folder or file paths in your personal or corporate enviroment. 

In the coming steps we will:
1. Explore our delta_log files
2. Open a delta log file to explore the metaData and CommitInfo
3. Extract, join, and analyze the data to create a human readable record of the schema evolution of our bronze layer table

## What is a delta_log file?

Delta log or transaction log files are a record of changes that occur on a delta table. 

More concretely, each delta log file represents an atomic change that takes place on the underlying Parquet files that make up the delta table. As Parquet files are inherently immutable, meaning that the data in the file cannot be edited in place, the delta log simulates changes to the underlying parquet by replacing versions of that parquet file with new versions each time the delta table is updated. 

For our purposes we will be exploring meta data on schema that stored in the delta_log files.

You can read more on delta_log files at databricks [official article](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html).


## Querying to see the delta_log files in your FileStore

We can use the % fs ls followed by our Databricks FileStore path to see the log files we created in ingesting data into our bronze layer table bronze_tj_fact_revenue. 

![image](https://github.com/user-attachments/assets/4bf4b792-b072-4721-b5df-e774122e1d27)

## Opening a delta_log file
