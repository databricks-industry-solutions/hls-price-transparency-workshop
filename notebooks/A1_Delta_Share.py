# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Price Transparency Delta Share
# MAGIC 
# MAGIC This notebook includes inline delta share credentials that will work through **31 MAR 2023**. This is to provide access to workshop attendees who would like to evaluate spark_price_transparency schemas without going through all `run_import`, `run_ingest`, and `run_analytic` tasks.
# MAGIC 
# MAGIC Let's take a look at the tables that were shared from the workshop on 21 MAR 2023:

# COMMAND ----------

from delta_sharing.protocol import DeltaSharingProfile
pt_config = {'share_credentials_version': 1,
             'endpoint': "https://eastus2-c2.azuredatabricks.net/api/2.0/delta-sharing/metastores/3e06dc8f-51a0-4e0c-a9df-16bfb4c0c00a",
             'bearer_token': "5wYEY2UKN2K3f_ZAHk_ZWvXPEKRmPuzr5WWNXIr_VSMt1yPuuranh4KDTJ25g86e",
             'expiration_time': "2023-04-01T00:00:00.000Z"}
shareProfile = DeltaSharingProfile(**pt_config)
client = delta_sharing.SharingClient(shareProfile)
display(client.list_all_tables())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Save credetials for distributed use
# MAGIC 
# MAGIC To make these credentials available across the cluster, we need to save the credentials in a dbfs location that is accessible to users users. Since this token is provided to all users for evaluation, we can safely save this in the public location `dbfs:/tmp/pt_config.share`. A more restrictive location according to authorized users should be chosen when credential security requires.
# MAGIC 
# MAGIC **NOTE**: This config share write to DBFS only needs to be run once per workspace which will suffice for all users.

# COMMAND ----------

# DBTITLE 0,_
import json
pt_config = {'shareCredentialsVersion': 1,
             'endpoint': "https://eastus2-c2.azuredatabricks.net/api/2.0/delta-sharing/metastores/3e06dc8f-51a0-4e0c-a9df-16bfb4c0c00a",
             'bearerToken': "5wYEY2UKN2K3f_ZAHk_ZWvXPEKRmPuzr5WWNXIr_VSMt1yPuuranh4KDTJ25g86e",
             'expirationTime': "2023-04-01T00:00:00.000Z"}
share_config_path = "dbfs:/tmp/pt_config.share"
dbutils.fs.rm(share_config_path)
dbutils.fs.put(share_config_path,json.dumps(pt_config))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Confirm Credentials Work
# MAGIC 
# MAGIC We will now we able to use our credential path with share, schema, and table name from the client table list to access whatever table we'd like. Below we'll show the SQL and python api to query our table of contents data in `pt_stage.index_reports`. You can find more features on Delta Share in the [docs](https://docs.databricks.com/data-sharing/read-data-open.html#read-data-shared-using-delta-sharing-open-sharing).

# COMMAND ----------

dat = spark.read.format("deltaSharing").load("dbfs:/tmp/pt_config.share#price-transparency-workshop.pt_stage.index_reports").limit(2)
display(dat)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.index_reports;
# MAGIC CREATE TABLE IF NOT EXISTS default.index_reports USING deltaSharing LOCATION "dbfs:/tmp/pt_config.share#price-transparency-workshop.pt_stage.index_reports";
# MAGIC SELECT * FROM index_reports LIMIT 2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create temp view for all shared tables
# MAGIC 
# MAGIC While this is exceptionally convenient relative to having to load the data in another way, it is a bit verbose for workshop attendees to type out. So provided is the snippet to iterate through the client table list and create a temp view for all the tables:

# COMMAND ----------

for tbl in client.list_all_tables():
    spark.read.format("deltaSharing").load(f'dbfs:/tmp/pt_config.share#{tbl.share}.{tbl.schema}.{tbl.name}').createOrReplaceTempView(tbl.name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Explore!
# MAGIC 
# MAGIC Take the rest of this notebook to inspect the Spark Price Transparency table schemas. Please provide any feedback you have to your Databricks account team. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     reporting_entity_name,
# MAGIC     COUNT(*) cnt
# MAGIC FROM in_rate
# MAGIC GROUP BY 1;
