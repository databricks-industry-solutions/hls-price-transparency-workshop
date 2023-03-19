# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Allowed-Amounts Curation
# MAGIC 
# MAGIC At the top level of the Allowed-Amounts [schema](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/allowed-amounts) is a single array, `out_of_network`. Including the file header that means we'll need to ingest into two ingest tables; `aa_header`, `aa_network`. However, these tables are relatively deeply nested and would result in an excessive number of records if we were to fully denormalize.

# COMMAND ----------

from spark_price_transparency.allowed_amounts.curate_viz import get_curate_html
displayHTML(get_curate_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Import of Allowed-Amounts Files
# MAGIC 
# MAGIC Also taken from our Table-Of-Contents analytic table, `pt_stage.index_reports`, are the Allowed-Amounts report urls. In our example, we'll again look for Databricks Allowed-Amounts for out of network.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC Once we've identified, our urls of interest, we can import them using `run_import` methods which will download the file. To address other url list format scenarios, multiple variants of **run_import** exist. Each of these will import into `<guide.pt_raw.locationUri>/mth=<guide.mth>/scenario=allowed-amounts` directory:
# MAGIC 
# MAGIC | run_import method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_raw.allowed_amounts.**run_import_url**(*url: str*) | Import a single Allowed-Amounts File from `pt_stage.index_reports` a location string. |
# MAGIC | guide.pt_raw.allowed_amounts.**run_import_urls**(*urls: [str]*)  | Import multiple Allowed-Amounts Files from python list of `pt_stage.index_reports` location strings. |
# MAGIC | guide.pt_raw.allowed_amounts.**run_import_pdf**(*url_pdf: pandas.DataFrame*) | Import multiple Allowed-Amounts files from pandas columns of `pt_stage.index_reports` location strings. </br> If there is no `url` column, it will attempt to use `location` column, if there is no `location` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.allowed_amounts.**run_import_df**(*url_pd: pyspark.sql.DataFrame*) | Import multiple Allowed-Amounts files from Spark DataFrame Column of `pt_stage.index_reports` location strings. </br> If there is no `url` column, it will attempt to use `location` column, if there is no `location` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.allowed_amounts.**run_import**() | Imports all of the `pt_stage.index_reports` location (urls) that have not been imported yet for the `guide.mth`. |

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide(mth=202303)

# COMMAND ----------

from pyspark.sql.functions import col, lit, explode

url_databricks_plan = spark.table('pt_stage.index_reports') \
                               .filter(col('file_type') == lit('allowed-amounts')) \
                               .withColumn('reporting_plan', explode(col('reporting_plans'))) \
                               .filter(col('reporting_plan.plan_name').contains('Databricks')) \
                               .filter(col('location').contains('oap')) \
                               .drop('reporting_plan') \
                               .withColumnRenamed('location', 'url')

display(url_databricks_plan)

# COMMAND ----------

ptg.pt_raw.allowed_amounts.run_import_df(url_databricks_plan)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Allowed-Amounts Files / Meta
# MAGIC 
# MAGIC Similar to In-Network-Rates, Allowed-Amounts also has a way to inspect raw files metadata, `guide.pt_raw.allowed_amounts.meta`.
# MAGIC 
# MAGIC **NOTE**: The `guide.pt_raw.allowed_amounts.meta` dataframe includes two more columns than `guide.pt_raw.in_network_rates.files`; `ingested`, `reporting_entity_name`. These fields will be populated accordingly once the file is ingested and can be used as an indicator to identify raw files that have yet to be ingested.

# COMMAND ----------

display(ptg.pt_raw.allowed_amounts.files)

# COMMAND ----------

display(ptg.pt_raw.allowed_amounts.meta)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Ingest Streaming Job for Allowed-Amounts file
# MAGIC 
# MAGIC While we may have the raw json files for the Allowed-Amounts file, it is still in a json format which isn't very performant. So we will want to ingest this json file into the following delta tables:
# MAGIC  * `pt_stage.aa_header`
# MAGIC  * `pt_stage.aa_network`
# MAGIC 
# MAGIC The ingest job will make use of a custom streaming source, [hls-payer-mrf-sparkstreaming](https://github.com/databricks-industry-solutions/hls-payer-mrf-sparkstreaming) and write both tables above in the a single streaming query. We offer multiple ways to run this query:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.allowed_amounts.**file_ingest_query**(*file_path: str*) | Returns a **started** streaming query. This can be helpful when inspecting queries, but will continue to run (idle) after all records the file path have already been ingested. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_ingest_file**(*file_path: str*) | Ingest a single Allowed-Amounts file from an Allowed-Amounts file_path string. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_ingest_files**(*file_paths: [str]*)  | Ingest multiple Allowed-Amounts files from python list of Allowed-Amounts file_path strings. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_ingest_pdf**(*file_path_pdf: pandas.DataFrame*) | Ingest multiple Allowed-Amounts files from pandas DataFrame of Allowed-Amounts file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_ingest_df**(*file_path_pd: pyspark.sql.DataFrame*) | Ingest multiple Allowed-Amounts files from spark DataFrame of Allowed-Amounts file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_ingest**() | Ingests all Allowed-Amounts files that have been imported, but have yet to be ingested for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Target ingest tables are partitioned by `mth` and will write into the `mth` partition values assigend to `guide.mth` independent of it's actual path. 
# MAGIC 
# MAGIC **NOTE**: Even though we are ingesting into multiple tables, these tables are written from a single streaming dataframe making use of [foreachPartion](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.foreachPartition.html) with multiple [delita merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) operations.
# MAGIC 
# MAGIC **NOTE**: If multiple files are run at once, these files are all ingested in series. Discovery underway to run multiple file ingests concurrently.

# COMMAND ----------

ptg.pt_stage.allowed_amounts.run_ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Allowed-Amounts Ingest Tables
# MAGIC 
# MAGIC We can now use spark sql to inspect the contents of our ingested data; `pt_stage.aa_header`, and `pt_stage.aa_network`.
# MAGIC 
# MAGIC **NOTE**: It is possible you will get ingest table records so large that they will not display and you may see an error message. Consider using [element_at](https://spark.apache.org/docs/2.4.1/api/sql/#element_at) to inspect only one element of an array in a query if this happens.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.aa_header LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.aa_network LIMIT 2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Analytic Table Merge for Allowed-Amount Files
# MAGIC 
# MAGIC While it is much more performant to read the Allowed-Amounts data in the ingest forms, there are two outstanding improvements to be made:
# MAGIC  - Convert the three ingest tables into two analytic tables for analyses
# MAGIC  - explode nested records so that there are fewer array transforms to deal with
# MAGIC 
# MAGIC There are multiple `run_analytic` variants. You can see the complete list of methods below:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.allowed_amounts.**run_analytic_file**(*file_name: str*) | Merge into analytic form a single Allowed-Amount File from a Allowed-Amount file_name string. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_analytic_files**(*file_names: [str]*)  | Merge into analytic form multiple Allowed-Amount Files from python list of Allowed-Amount file_name strings. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_analytic_pdf**(*file_pdf: pandas.DataFrame*) | Merge into analytic form multiple Allowed-Amount Files from pandas DataFrame of Allowed-Amount file_name strings. </br> If there is no `file_name` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_analytic_df**(*file_pd: pyspark.sql.DataFrame*) | Merge into analytic form multiple Allowed-Amount files from spark DataFrame of Allowed-Amount file_name strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.allowed_amounts.**run_analytic**() | Merge into analytic form all files that have been ingested, but have yet to be merged into the analytic table for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Target analytic tables are partitioned by `mth` and will write into the `mth` partition values assigend to `guide.mth` independent of it's actual path. 

# COMMAND ----------

ptg.pt_stage.allowed_amounts.run_analytic()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Analytic Tables for Allowed-Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.out_code LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.out_amount LIMIT 2;
