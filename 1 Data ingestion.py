# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # APJuice Lakehouse Platform
# MAGIC 
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### APJ Data Sources
# MAGIC 
# MAGIC For this exercise we will be starting to implement Lakehouse platform for our company, APJuice.
# MAGIC 
# MAGIC APJuice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
# MAGIC 
# MAGIC For the first part of the exercise we will be focusing on an export of Store Locations table that has been saved as `csv` file.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using [Databricks Notebooks workflow](https://docs.databricks.com/notebooks/notebook-workflows.html) element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system or reading [Databricks Secrets](https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-secrets)

# COMMAND ----------

dbutils.widgets.dropdown("uc_status", "Enabled", ["Enabled", "Disabled"], "Unity Catalog")

# COMMAND ----------

uc_status= dbutils.widgets.get("uc_status")
print("Unity Catalog : {}".format(uc_status))

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0, {"uc_status": uc_status}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]


print("Local data path is {}".format(local_data_path))
print("DBFS path is {}".format(dbfs_data_path))
if uc_status == 'Enabled':
  catalog_name = setup_responses[3]
  print("Catalog name is {}".format(catalog_name))
  spark.sql(f"USE CATALOG {catalog_name};")
print("Database name is {}".format(database_name))


spark.sql(f"USE {database_name};")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC Let's load store locations data to Delta Table. In our case we don't want to track any history and opt to overwrite data every time process is running.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Delta Table
# MAGIC 
# MAGIC ***Load Store Locations data to Delta Table***
# MAGIC 
# MAGIC In our example CRM export has been provided to us as a CSV file and uploaded to `dbfs_data_path` location. It could also be your S3 bucket, Azure Storage account or Google Cloud Storage. 
# MAGIC 
# MAGIC We will not be looking at how to set up access to files on the cloud environment in today's workshop.
# MAGIC 
# MAGIC 
# MAGIC For our APJ Data Platform we know that we will not need to keep and manage history for this data so creating table can be a simple overwrite each time ETL runs.
# MAGIC 
# MAGIC 
# MAGIC Let's start with simply reading CSV file into DataFrame

# COMMAND ----------

dataPath = f"{dbfs_data_path}/stores.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("quote", "\"") \
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data is in a DataFrame, but not yet in a Delta Table. Still, we can already use SQL to query data or copy it into the Delta table

# COMMAND ----------

# Creating a Temporary View will allow us to use SQL to interact with data

df.createOrReplaceTempView("stores_csv_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from stores_csv_file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL DDL can be used to create table using view we have just created. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS stores;
# MAGIC 
# MAGIC CREATE TABLE stores
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM stores_csv_file;
# MAGIC 
# MAGIC SELECT * from stores;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Update Delta Table
# MAGIC 
# MAGIC Provided dataset has address information, but no country name - let's add one!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC alter table stores
# MAGIC add column store_country string;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC update stores
# MAGIC set store_country = case 
# MAGIC   when id in ('SYD01', 'MEL01', 'BNE02','CBR01','PER01') then 'AUS' 
# MAGIC   when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' 
# MAGIC end;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select store_country, id, name from stores

# COMMAND ----------

# MAGIC %sql
# MAGIC update
# MAGIC   stores
# MAGIC set
# MAGIC   store_country = 'AUS'
# MAGIC where
# MAGIC   id = 'MEL02'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   store_country,
# MAGIC   count(id) as number_of_stores
# MAGIC from
# MAGIC   stores
# MAGIC group by
# MAGIC   store_country

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Track Data History - Time travel
# MAGIC 
# MAGIC 
# MAGIC Delta Tables keep all changes made in the delta log we've seen before. There are multiple ways to see that - e.g. by running `DESCRIBE HISTORY` for a table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY stores

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Having all this information and old data files mean that we can **Time Travel**!  You can query your table at any given `VERSION AS OF` or  `TIMESTAMP AS OF`.
# MAGIC 
# MAGIC Let's check again what table looked like before we ran last update

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select store_country, * from stores VERSION AS OF 2 where id = 'MEL02';
