# Delta_Log_History_and_Unpacking_JSON_Dict_Databricks

### Extracting Data from delta_log files to Report on Schema Evolution

The data displayed in this repository is based on results from running notebooks 1-3 located in my related repository [Databricks_Schema_Evolution_With_Autoloader](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader).

You may also execute the following steps with your own delta table data with some simple simple modifications of the relevant folder or file paths in your personal or corporate enviroment. 

In the coming steps we will explore our delta_log, open a delta log file to explore the metaData and CommitInfo, and extract, join and analyze the data to create a human readable record of schema evolution of our bronze layer table. 


## Querying to see the delta_log files in your FileStore

We can use the % fs ls followed by our Databricks FileStore path to see the log files we created in ingesting data into our bronze layer table bronze_tj_fact_revenue.

![image](https://github.com/user-attachments/assets/4bf4b792-b072-4721-b5df-e774122e1d27)
