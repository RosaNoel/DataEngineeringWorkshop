# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # APJuice Lakehouse Platform
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC 
# MAGIC In this Notebook we will see how to implement the Medalion Architecture on your Lakehouse. 
# MAGIC 
# MAGIC Some of the things we will look at are:
# MAGIC * Using Auto-loader
# MAGIC    * Batch and Stream Ingestion
# MAGIC    * Rescued data
# MAGIC * Optimizing tables for specific query pattern using OPTIMIZE and ZORDER
# MAGIC * Incremental updates using MERGE
# MAGIC * Scheduling jobs
# MAGIC * Data lineage (requires Unity Catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ### APJ Data Sources
# MAGIC 
# MAGIC For this exercise we will be starting to implement Lakehouse platform for our company, APJuice.
# MAGIC 
# MAGIC APJuice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
# MAGIC 
# MAGIC For this part of the exercise we will be processing 3 existing dimensions and sales transactions datasets. Files will be a mix of `csv` and `json` files and our goal is to have **incremental updates** for sales table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup
# MAGIC 
# MAGIC We will be using [Databricks Notebooks workflow](https://docs.databricks.com/notebooks/notebook-workflows.html) element to set up environment for this exercise. 
# MAGIC 
# MAGIC `dbutils.notebook.run()` command will run another notebook and return its output to be used here.
# MAGIC 
# MAGIC `dbutils` has some other interesting uses such as interacting with file system (check our `dbutils.fs.rm()` being used in the next cell) or to read Secrets.

# COMMAND ----------

dbutils.widgets.dropdown("uc_status", "Enabled", ["Enabled", "Disabled"], "Unity Catalog")

# COMMAND ----------

uc_status= dbutils.widgets.get("uc_status")
print("Unity Catalog : {}".format(uc_status))

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0, {"uc_status": uc_status}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

bronze_table_path = f"{dbfs_data_path}tables/bronze"
silver_table_path = f"{dbfs_data_path}tables/silver"
gold_table_path = f"{dbfs_data_path}tables/gold"

autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"

# Remove all files from location in case there were any
dbutils.fs.rm(bronze_table_path, recurse=True)
dbutils.fs.rm(silver_table_path, recurse=True)
dbutils.fs.rm(gold_table_path, recurse=True)

print("Local data path is {}".format(local_data_path))
print("DBFS path is {}".format(dbfs_data_path))
if uc_status == 'Enabled':
  catalog_name = setup_responses[3]
  print("Catalog name is {}".format(catalog_name))
  spark.sql(f"USE CATALOG {catalog_name};")
print("Database name is {}".format(database_name))
spark.sql(f"USE DATABASE {database_name};")
# print("Bronze Table Location is {}".format(bronze_table_path))
# print("Silver Table Location is {}".format(silver_table_path))
# print("Gold Table Location is {}".format(gold_table_path))





# COMMAND ----------

# MAGIC %run ./Utils/Define-Functions $uc_status=$uc_status

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceBootcampContext.png?raw=true" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer 
# MAGIC 
# MAGIC We have already seen the store locations dataset. Let's redo the work this time using suggested Delta Architecture steps

# COMMAND ----------

spark.sql(f"USE DATABASE {database_name};")
data_file_location = f"{dbfs_data_path}/stores.csv"

bronze_table_name = "bronze_store_locations"
silver_table_name = "dim_locations"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(data_file_location)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
select *, 
case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code
from {bronze_table_name}
""")

spark.sql(f"DROP TABLE IF EXISTS {silver_table_name};")


silver_df.write \
  .mode("overwrite") \
  .saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We have 2 more dimension tables that can be added to the Lakehouse without many data changes - dim_customers and dim_products.

# COMMAND ----------

data_file_location = f"{dbfs_data_path}/users.csv"

bronze_table_name = "bronze_customers"
silver_table_name = "dim_customers"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(data_file_location)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
SELECT store_id || "-" || cast(id as string) as unique_id, id, store_id, name, email FROM {bronze_table_name}
""")

spark.sql(f"DROP TABLE IF EXISTS {silver_table_name};")


silver_df.write \
  .mode("overwrite") \
  .saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC And repeat for dim_products - note that this time our input file is json and not csv

# COMMAND ----------

data_file_location = f"{dbfs_data_path}/products.json"

bronze_table_name = "bronze_products"
silver_table_name = "dim_products"

df = spark.read\
  .json(data_file_location)

spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name};")

df.write \
  .mode("overwrite") \
  .saveAsTable(bronze_table_name)

silver_df = spark.sql(f"""
select * from {bronze_table_name}
""")

spark.sql(f"DROP TABLE IF EXISTS {silver_table_name};")

silver_df.write \
  .mode("overwrite") \
  .saveAsTable(silver_table_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Using Autoloader
# MAGIC 
# MAGIC Easy way to bring incremental data to our Delta Lake is by using **autoloader**.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Prepare for the first autoloader run - as this is an example Notebook, we can delete all the files and tables before running it.

# COMMAND ----------

import pyspark.sql.functions as F

checkpoint_path = f'{local_data_path}/_checkpoints'
schema_path = f'{local_data_path}/_schema'
# write_path = f'{bronze_table_path}/bronze_sales'

spark.sql("drop table if exists bronze_sales")

refresh_autoloader_datasets = True

if refresh_autoloader_datasets:
  # Run these only if you want to start a fresh run!
  dbutils.fs.rm(checkpoint_path,True)
  dbutils.fs.rm(schema_path,True)
#   dbutils.fs.rm(write_path,True)
  dbutils.fs.rm(autoloader_ingest_path, True)
  
  dbutils.fs.mkdirs(autoloader_ingest_path) #This would be a cloud storage location
  
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202110.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202111.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202112.json", autoloader_ingest_path)



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_sales;
# MAGIC CREATE TABLE IF NOT EXISTS bronze_sales;

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, exported_ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 

streaming_autoloader = df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .table('bronze_sales')


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Check how many records were inserted to `bronze_sales` table - calculated column `file_path` is a good way to see it

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select file_path, count(*) number_of_records
# MAGIC from bronze_sales
# MAGIC group by file_path;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we have new data files arriving - rerunning autoloader cell will only process those yet unseen files. 
# MAGIC You can try it out by running `get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-01')` and then re-running autoloader cell.

# COMMAND ----------

get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-03')
spark.sql(f"USE DATABASE {database_name};")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  * from bronze_sales limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Silver Layer
# MAGIC 
# MAGIC Now that we have a bronze table ready - let's a create silver one! 
# MAGIC 
# MAGIC We can start by using same approach as for the dimension tables earlier - clean and de-duplicate data from bronze table, rename columns to be more business friendly and save it as silver table.

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create or replace view v_silver_sales 
# MAGIC as 
# MAGIC with with_latest_record_id as (
# MAGIC   select
# MAGIC     *,
# MAGIC     row_number() over (
# MAGIC       partition by SaleID
# MAGIC       order by
# MAGIC         coalesce(exported_ts, 0) desc
# MAGIC     ) as latest_record
# MAGIC   from
# MAGIC     bronze_sales
# MAGIC ),
# MAGIC newest_records as (
# MAGIC   select
# MAGIC     saleID as id,
# MAGIC     from_unixtime(ts) as ts,
# MAGIC     Location as store_id,
# MAGIC     CustomerID as customer_id,
# MAGIC     location || "-" || cast(CustomerID as string) as unique_customer_id,
# MAGIC     OrderSource as order_source,
# MAGIC     STATE as order_state,
# MAGIC     SaleItems as sale_items
# MAGIC   from
# MAGIC     with_latest_record_id
# MAGIC   where
# MAGIC     latest_record = 1
# MAGIC )
# MAGIC select
# MAGIC   *,
# MAGIC   sha2(concat_ws(*, '||'), 256) as row_hash -- add a hash of all values to easily pick up changed rows
# MAGIC from
# MAGIC   newest_records

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC drop table if exists silver_sales;
# MAGIC 
# MAGIC create table silver_sales 
# MAGIC as
# MAGIC select * from v_silver_sales;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silver_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Sales table is nice, but we also have sales items information object that can be split into rows for easier querying

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace view v_silver_sale_items 
# MAGIC as 
# MAGIC with itemised_records as (
# MAGIC   select
# MAGIC     *,
# MAGIC     posexplode(
# MAGIC       from_json(
# MAGIC         sale_items,
# MAGIC         'array<struct<id:string,size:string,notes:string,cost:double,ingredients:array<string>>>'
# MAGIC       )
# MAGIC     )
# MAGIC   from
# MAGIC     v_silver_sales
# MAGIC ),
# MAGIC all_records as (
# MAGIC   select
# MAGIC     id || "-" || cast(pos as string) as id,
# MAGIC     id as sale_id,
# MAGIC     store_id,
# MAGIC     pos as item_number,
# MAGIC     col.id as product_id,
# MAGIC     col.size as product_size,
# MAGIC     col.notes as product_notes,
# MAGIC     col.cost as product_cost,
# MAGIC     col.ingredients as product_ingredients
# MAGIC   from
# MAGIC     itemised_records
# MAGIC )
# MAGIC select
# MAGIC   *,
# MAGIC   sha2(concat_ws(*, '||'), 256) as row_hash
# MAGIC from
# MAGIC   all_records

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists silver_sale_items;
# MAGIC 
# MAGIC create table silver_sale_items
# MAGIC as
# MAGIC select * from v_silver_sale_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_sale_items;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### OPTIMIZE
# MAGIC 
# MAGIC 
# MAGIC Run a query to find a specific order in `silver_sale_items` table and note query execution time. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sale_items 
# MAGIC where sale_id = '00139294-b5c5-4af1-9b4c-181c1911ad16';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we know that most of the queries will be using `sale_id` filter - we can optimize this table by running `ZORDER` on that column.
# MAGIC 
# MAGIC Running `OPTIMIZE` on a table on Delta Lake on Databricks can improve the speed of read queries from a table by coalescing small files into larger ones. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize silver_sale_items
# MAGIC zorder by sale_id

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_sale_items 
# MAGIC where sale_id = '00139294-b5c5-4af1-9b4c-181c1911ad16';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists gold_country_sales;
# MAGIC 
# MAGIC create table gold_country_sales 
# MAGIC as 
# MAGIC select l.country_code, date_format(sales.ts, 'yyyy-MM') as sales_month, sum(product_cost) as total_sales, count(distinct sale_id) as number_of_sales
# MAGIC from silver_sale_items s 
# MAGIC   join dim_locations l on s.store_id = l.id
# MAGIC   join silver_sales sales on s.sale_id = sales.id
# MAGIC group by l.country_code, date_format(sales.ts, 'yyyy-MM');

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gold_country_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists gold_top_customers;
# MAGIC 
# MAGIC create table gold_top_customers 
# MAGIC as
# MAGIC select s.store_id, ss.unique_customer_id, c.name, sum(product_cost) total_spend 
# MAGIC from silver_sale_items s 
# MAGIC   join silver_sales ss on s.sale_id = ss.id
# MAGIC   join dim_customers c on ss.unique_customer_id = c.unique_id
# MAGIC where ss.unique_customer_id is not null 
# MAGIC group by s.store_id, ss.unique_customer_id, c.name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- get top 3 customers for each store
# MAGIC with ranked_customers as (
# MAGIC select store_id, name as customer_name, total_spend as customer_spend, rank() over (partition by store_id order by total_spend desc) as customer_rank 
# MAGIC from gold_top_customers )
# MAGIC select * from ranked_customers
# MAGIC where customer_rank <= 3
# MAGIC order by store_id, customer_rank

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Stop streaming autoloader to allow our cluster to shut down.

# COMMAND ----------

streaming_autoloader.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Scheduled Updates

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can schedule this Notebook to run every day.
