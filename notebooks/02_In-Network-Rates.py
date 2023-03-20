# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # In Network Rates Curation
# MAGIC 
# MAGIC The In-Network-Rates curation is more involed than the Table-of-Contents curations. At the top level of the [schema](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates) are two arrays, `in_network` and `provider_refrence`. Including the file header that means we'll need to ingest into three ingest tables; `inr_header`, `inr_network`, `inr_provider`. However, these tables are relatively deeply nested and would result in an excessive number of records if we were to fully denormalize. So instead we will flatten and and push network data into two analytic tables `in_coverage` and `in_rate`. The provider data we will put into the analytic table `in_provider`. In provider will actually require updates since it will have records details come from both `inr_provider` and `pr_provider`. Lastly, we'll have a separate table for when only the provider reference groups file location is provided. In that scenario which we will run through below, those file locations are written to `pt_stage.in-pr_loc`.

# COMMAND ----------

from spark_price_transparency.in_network_rates.curate_viz import get_curate_html
displayHTML(get_curate_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Import of In-Network-Rates file
# MAGIC 
# MAGIC Continuing from our table of contents work so far, we will use the table of contents analytic table to find an In-Network-Rates File of interest. As example, we'll find the **Databrick's** plan **Open Access Plus** (oap) in-network-rates from **Cigna**.
# MAGIC 
# MAGIC **Note**: It looks like the rates negotiated in this plan are used by a lot of companies.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC Once we've identified, our urls of interest, we can import them using `run_import` methods which will import the file. To address other url list format scenarios, multiple variants of **run_import** exist. Each of these will import into `<guide.pt_raw.locationUri>/mth=<guide.mth>/scenario=in-network-rates` directory:
# MAGIC 
# MAGIC | run_import method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_raw.in_network_rates.**run_import_url**(*url: str*) | Import a single In-Network-Rates File from In-Network-Rates url string. |
# MAGIC | guide.pt_raw.in_network_rates.**run_import_urls**(*urls: [str]*)  | Import multiple In-Network-Rates Files from python list of In-Network-Rates url strings. |
# MAGIC | guide.pt_raw.in_network_rates.**run_import_pdf**(*url_pdf: pandas.DataFrame*) | Import multiple In-Network-Rates files from python list of In-Network-Rates url strings. </br> If there is no `url` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.in_network_rates.**run_import_df**(*url_pd: pyspark.sql.DataFrame*) | Import multiple In-Network-Rates Files from pandas DataFrame Column of In-Network-Rates url strings. </br> If there is no `url` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.in_network_rates.**run_import**() | Imports all In-Network-Rates urls that have note been imported yet for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Databricks does not plan to develop any high capacity import functionality. Work with you SIs on options to import data if single threaded import doesn't meet your business needs.

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide(mth=202303)

# COMMAND ----------

from pyspark.sql.functions import col, lit, explode

url_databricks_plan = spark.table('pt_stage.index_reports') \
                               .filter(col('file_type') == lit('in-network-rates')) \
                               .withColumn('reporting_plan', explode(col('reporting_plans'))) \
                               .filter(col('reporting_plan.plan_name').contains('Databricks')) \
                               .filter(col('location').contains('oap')) \
                               .drop('reporting_plan') \
                               .withColumnRenamed('location', 'url')

display(url_databricks_plan)

# COMMAND ----------

ptg.pt_raw.in_network_rates.run_import_df(url_databricks_plan)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect In-Network-Rates Files / Meta
# MAGIC 
# MAGIC Similar to table of contents, In-Network-Rates also has a way to inspect raw files metadata, `guide.pt_raw.in_network_rates.meta`.
# MAGIC 
# MAGIC **NOTE**: The `guide.pt_raw.in_network_rates.meta` dataframe includes two more columns than `guide.pt_raw.in_network_rates.files`; `ingested`, `reporting_entity_name`. These fields will be populated accordingly once the file is ingested and can be used as an indicator to identify raw files that have yet to be ingested. That's what we'll use here to see the path of our raw, un-ingested, In-Network-Rates file.

# COMMAND ----------

display(ptg.pt_raw.in_network_rates.files)

# COMMAND ----------

display(ptg.pt_raw.in_network_rates.meta)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Ingest Streaming Job for In-Network-Rates file
# MAGIC 
# MAGIC While we may have the raw json files for the In-Network-Rates file, it is still in a json format which isn't very performant. So we will want to ingest this json file into the following delta tables:
# MAGIC  * `pt_stage.inr_header`
# MAGIC  * `pt_stage.inr_network`
# MAGIC  * `pt_stage.inr_provider`
# MAGIC 
# MAGIC The ingest job will make use of a custom streaming source, [hls-payer-mrf-sparkstreaming](https://github.com/databricks-industry-solutions/hls-payer-mrf-sparkstreaming) and write all three tables above in the a single streaming query. We offer multiple ways to run this query:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.in_network_rates.**file_ingest_query**(*file_path: str*) | Returns a **started** streaming query. This can be helpful when inspecting queries, but will continue to run (idle) after all records the file path have already been ingested. |
# MAGIC | guide.pt_stage.in_network_rates.**run_ingest_file**(*file_path: str*) | Ingest a single In-Network-Rates File from a In-Network-Rates file_path string. |
# MAGIC | guide.pt_stage.in_network_rates.**run_ingest_files**(*file_paths: [str]*)  | Ingest multiple In-Network-Rates Files from python list of In-Network-Rates file_path strings. |
# MAGIC | guide.pt_stage.in_network_rates.**run_ingest_pdf**(*file_path_pdf: pandas.DataFrame*) | Ingest multiple In-Network-Rates Files from pandas DataFrame of In-Network-Rates file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.in_network_rates.**run_ingest_df**(*file_path_pd: pyspark.sql.DataFrame*) | Ingest multiple In-Network-Rates Files from spark DataFrame of In-Network-Rate file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.in_network_rates.**run_ingest**() | Ingests all In-Network-Rates files that have been imported, but have yet to be ingested for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Target ingest tables are partitioned by `mth` and will write into the `mth` partition values assigend to `guide.mth`.
# MAGIC 
# MAGIC **NOTE**: Due to the fact the these json files are single record with heavily nested entities, the convention in `spark_price_transparency` is to ingest all files such that there is a table for the json header and a table for each array element in the top level json. For In-Network-Rates schema there are two arrays in the json top level called, `in_network` and `provider_references`, which we will write to `pt_stage.inr_network` and `pt_stge.inr_provider` respectively.
# MAGIC 
# MAGIC **NOTE**: Even though we are ingesting into multiple tables, these tables are written from a single streaming dataframe making use of [foreachPartion](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.foreachPartition.html) with multiple [delita merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) operations.
# MAGIC 
# MAGIC **NOTE**: In development, the `run_ingest` will be able to ingest multiple files in the same streaming job with concurrancy managed as an arguement. Currently, these files are all ingested in series.

# COMMAND ----------

ptg.pt_stage.in_network_rates.run_ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect In-Network-Rates Ingest Tables
# MAGIC 
# MAGIC We can now use spark sql to inspect the contents of our ingested data; `pt_stage.inr_header`, `pt_stage.inr_network`, and `pt_stage.inr_provider`.
# MAGIC 
# MAGIC **NOTE**: Sometimes you will get ingest table records so large that they will not display and you may see an error message. Consider using [element_at](https://spark.apache.org/docs/2.4.1/api/sql/#element_at) to inspect only one element of an array in a query if this happens.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recall from table of contents, you will likely see two entries in the header table one for beginning of file and one for end of file
# MAGIC SELECT * FROM pt_stage.inr_header;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.inr_network LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.inr_provider limit 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Analytic Table Merge for In-Network-Rates Files
# MAGIC 
# MAGIC While it is much more performant to read the In-Network-Rates data in the ingest forms, there are two outstanding improvements to be made:
# MAGIC  - Convert the three ingest tables into four analytic tables for analyses
# MAGIC  - explode nested records so that there are fewer array transforms to deal with
# MAGIC 
# MAGIC There are multiple `run_analytic` variants. You can see the complete list of methods below:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.in_network_rates.**run_analytic_file**(*file_name: str*) | Merge into analytic form a single In-Network-Rates File from a In-Network-Rates file_name string. |
# MAGIC | guide.pt_stage.in_network_rates.**run_analytic_files**(*file_names: [str]*)  | Merge into analytic form multiple In-Network-Rates Files from python list of In-Network-Rates file_name strings. |
# MAGIC | guide.pt_stage.in_network_rates.**run_analytic_pdf**(*file_pdf: pandas.DataFrame*) | Merge into analytic form multiple In-Network-Rates files from pandas DataFrame of In-Network-Rates file_name strings. </br> If there is no `file_name` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.in_network_rates.**run_analytic_df**(*file_pd: pyspark.sql.DataFrame*) | Merge into analytic form multiple In-Network-Rates files from spark DataFrame of In-Netowrk-Rates file_name strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.in_network_rates.**run_analytic**() | Merge into analytic form all files that have been ingested, but have yet to be merged into the analytic table for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Target analytic tables are partitioned by `mth` and will write into the `mth` partition values assigend to `guide.mth`.

# COMMAND ----------

ptg.pt_stage.in_network_rates.run_analytic()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect In-Network-Rates Analytic Tables
# MAGIC 
# MAGIC We can now use spark sql to inspect the In-Network-Rates Analytic Tables:
# MAGIC 
# MAGIC | table | description |
# MAGIC | ----- | ----------- |
# MAGIC | `pt_stage.in_coverage` | coverage will include all of the billing codes included in a plan, whether type ffs, bundle, or capitation. |
# MAGIC | `pt_stage.in_rate` | The rates that apply for a specific entity, coverage, provider, service code combination. This table will have the high record count within the In-Network-Rates schema. |
# MAGIC | `pt_stage.in_provider` | When provider group details are provided, either from `inr_network` or from `pr_provider`, those details will be saved here. `in_provider` is unique amoung the In-Network-Rates Analytic tables because it does not maintain a `file_name` field. This is because the provider group details can come from multipleIn-Network-Rate Files or even multiple Provider-Reference Files.
# MAGIC | `pt_stage.in_pr_loc` | This will collect all locations of Provider-Reference files that will be needed to be imported for the Provider-Reference workflow. |
# MAGIC 
# MAGIC **NOTE**: There are no records in `pt_stage.in_provider`. This is becuase we didn't have any provider details only provider locations in this plan. However, we'll inport the files from some of the locations in the next section on Provider-Refrence Files.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_coverage LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_rate LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shouldn't expect any records since this particular plan uses provider reference files
# MAGIC SELECT * FROM pt_stage.in_provider;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_pr_loc LIMIT 2;
