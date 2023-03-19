# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Provider-Reference Data Curation
# MAGIC 
# MAGIC The Provider-Reference curation is somewhat a continuation of the In-Network-Rates curation. Provider-Refrence Files are used when the In-network-Rates legal entity chooses to provide a reference location of the providers in a provider reference group instead of within the In-Network-Rates schema. As a consequence, we can only get these files if thier location is provided by the In-Network-Rates curation proces.

# COMMAND ----------

from spark_price_transparency.provider_reference.curate_viz import get_curate_html
displayHTML(get_curate_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Import of Provider-Reference Files
# MAGIC 
# MAGIC Continuing from our In-Network-Rates curation, we will use the records in `pt_stage.in_pr_loc` to identify providers of interest. In our example, we'll look for just a subset to be imported by filtering for CPT code **92562**. This will only return a subset of providers since is code isn't a service offered by all providers, but it will show how one might use the In-Network-Rates Analytic Tables to find a list of providers of interest.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC Once we've identified, our urls of interest, we can import them using `run_import` methods which will download the file. To address other url list format scenarios, multiple variants of **run_import** exist. Each of these will import into `<guide.pt_raw.locationUri>/mth=<guide.mth>/scenario=in-network-rates` directory:
# MAGIC 
# MAGIC | run_import method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_raw.provider_reference.**run_import_url**(*url: str*) | Import a single Provider-Reference File from `pt_stage.in_pr_loc` location string. |
# MAGIC | guide.pt_raw.provider_reference.**run_import_urls**(*urls: [str]*)  | Import multiple Provider-Reference Files from python list of `pt_stage.in_pr_loc` location strings. |
# MAGIC | guide.pt_raw.provider_reference.**run_import_pdf**(*url_pdf: pandas.DataFrame*) | Import multiple Provider-Reference-Files files in pandas column of `pt_stage.in_pr_loc` location strings. </br> If there is no `url` column, it will attempt to use `location` column, if there is no `location` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.provider_reference.**run_import_df**(*url_pd: pyspark.sql.DataFrame*) | Import multiple Provider-Reference Files from Spark DataFrame Column of `pt_stage.in_pr_loc` location strings. </br> If there is no `url` column, it will attempt to use `location` column, if there is no `location` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.provider_reference.**run_import**() | Imports all of the `pt_stage.in_pr_loc` location (urls) that have note been imported yet for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Currently, spark_price_transparency will onlly allow the import of urls that also exist in `pt_stage.in_pr_loc`. If this wasn't enforced, the reulsting json file will be orphaned from the plan(s) withwhich it is associated.

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide()

# COMMAND ----------

from pyspark.sql.functions import col, lit
pr_files = spark.table("pt_stage.in_pr_loc") \
                .join(spark.table("pt_stage.in_rate").join(spark.table("pt_stage.in_coverage").filter(col('issuer_billing_code.code') == lit('92562')), "sk_coverage", "left_semi"),
                      "sk_pr_loc", "left_semi").withColumnRenamed('location', 'url')
display(pr_files)

# COMMAND ----------

ptg.pt_raw.provider_reference.run_import_df(pr_files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Provider-Reference Files / Meta
# MAGIC 
# MAGIC Similar to table-of-contents and in-network-rates, we can inspect Provider-Reference Files using, `guide.pt_raw.provider_reference.files` and `guide.pt_raw.provider_reference.meta`.
# MAGIC 
# MAGIC **NOTE**: The `guide.pt_raw.in_network_rates.meta` dataframe includes two more columns than `guide.pt_raw.in_network_rates.files`; `ingested`, `reporting_entity_name`. These fields will be populated accordingly once the file is ingested and can be used as an indicator to identify raw files that have yet to be ingested. That's what we'll use here to see the path of our raw, un-ingested, in network rates file.

# COMMAND ----------

display(ptg.pt_raw.provider_reference.files.limit(2))

# COMMAND ----------

display(ptg.pt_raw.provider_reference.meta.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Ingest Batch Job for Provider-Reference files
# MAGIC 
# MAGIC While we may have the raw json files for the Provider-Reference files, it is still in a json format and in many small files which isn't performant. So we will want to ingest this json file into the following delta table:
# MAGIC  * `pt_stage.pr_provider`
# MAGIC 
# MAGIC The ingest jobs for Provider-Reference Files is a *batch job* instead of a structured streaming job like in the other schema. Regardless, it still follows the same method argument conventions.
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.provider_reference.**run_ingest_file**(*file_path: str*) | Ingest a single Provider-Reference File from a Provider-Reference file_path string. |
# MAGIC | guide.pt_stage.provider_reference.**run_ingest_files**(*file_paths: [str]*)  | Ingest multiple Provider-Reference Files from python list of Provider-Reference file_path strings. |
# MAGIC | guide.pt_stage.provider_reference.**run_ingest_pdf**(*file_path_pdf: pandas.DataFrame*) | Ingest multiple Provider-Reference Files from pandas DataFrame of Provider-Reference file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.provider_reference.**run_ingest_df**(*file_path_pd: pyspark.sql.DataFrame*) | Ingest multiple Provider-Reference Files from spark DataFrame of Provider-Reference file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.provider_reference.**run_ingest**() | Ingests all Provider-Reference Files that have been imported, but have yet to be ingested for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Target table is partitioned by `mth` and will write into the `mth` partition values assigend to `guide.mth` independent of it's actual path. 

# COMMAND ----------

file_path_df = ptg.pt_raw.provider_reference.meta.filter(col('file_name').contains('Cigna'))
ptg.pt_stage.provider_reference.run_ingest_df(file_path_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Provider-Reference Ingest Table
# MAGIC 
# MAGIC We can now use spark sql to inspect the contents of our ingested data; `pt_stage.pr_provider`. Notice, that this includes attributes from `pt_stage.in_pr_loc` including, `reporting_entity_name`,`sk_pr_loc`. These attributes were joined during ingest to simplify the `run_analytic` methods.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.pr_provider LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Analytic Table Merge for Provider-Reference
# MAGIC 
# MAGIC Provider-Reference `run_analytic` is different than all other since it only updates records that already exist within the In-Netowrk-Rates schema. Specifically, this will only update `pt_stage.in_rate`, and only insert into `pt_stage.in_provider`.
# MAGIC 
# MAGIC We can complete the transform and merge using `guide.pt_stage.provider_reference.run_analytic`. This new form will list all files in the table of contents as a separate row which will help facilitate methods to identify what amount of the table of contents files remain to be imported. However, as before, other `run_analytic` methods are available:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.provider_reference.**run_analytic_file**(*file_name: str*) | Merge into analytic form a single Provider-Reference File from a Provider-Reference file_name string. |
# MAGIC | guide.pt_stage.provider_reference.**run_analytic_files**(*file_names: [str]*)  | Merge into analytic form multiple Provider-Reference Files from python list of Provider-Reference file_name strings. |
# MAGIC | guide.pt_stage.provider_reference.**run_analytic_pdf**(*file_pdf: pandas.DataFrame*) | Merge into analytic form multiple Provider-Reference files from pandas DataFrame of Provider-Reference file_name strings. </br> If there is no `file_name` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.provider_reference.**run_analytic_df**(*file_pd: pyspark.sql.DataFrame*) | Merge into analytic form multiple Provider-Reference files from spark DataFrame of Provider-Reference file_name strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.provider_reference.**run_analytic**() | Merge into analytic form all Provider-Reference files that have been ingested, but have yet to be merged into the analytic table for the `guide.mth`. |

# COMMAND ----------

ptg.pt_stage.provider_reference.run_analytic()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Provider-Reference Analytic Run

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.pr_provider LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_rate WHERE sk_provider IS NOT NULL LIMIT 2;
