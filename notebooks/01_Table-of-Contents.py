# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Table of Contents Data Curation
# MAGIC 
# MAGIC The Table-of-Contents file is the entry point for other Price Transparency schemas. Once we complete `run_import`, `run_ingest`, `run_analytic`, we'll have a formatted delta table, `pt_stage.index_reports`, with listing of all In-Network-Rates and Allowed-Amounts Files to import.

# COMMAND ----------

from spark_price_transparency.table_of_contents.curate_viz import get_curate_html
displayHTML(get_curate_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Import of Table of Contents File
# MAGIC 
# MAGIC The [Table of Contents](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/table-of-contents) File schema includes the listing of all plan In-Network-Rates Files and Allowed-Amounts Files. If you are seeking to get a specific file by plan, this is the place to start. Here is a listing where some large payers keep thier Table of Contents files (although, there will be many more than just thsese):
# MAGIC 
# MAGIC  * [UnitedHealth Group](https://transparency-in-coverage.uhc.com/)
# MAGIC  * [Anthem](https://www.anthem.com/machine-readable-file/search/)
# MAGIC  * [Cigna](https://www.cigna.com/legal/compliance/machine-readable-files)
# MAGIC  * [Aetna](https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICSI/machine-readable-transparency-in-coverage?reportingEntityType=Third%20Party%20Administrator_6644&lock=true)
# MAGIC  * [Humana](https://developers.humana.com/syntheticdata/Resource/PCTFilesList?fileType=innetwork)
# MAGIC  
# MAGIC  ---
# MAGIC  
# MAGIC We'll use one of Databricks own health insurance provider, Cigna, as an example of how to ingest a table of contents.
# MAGIC  
# MAGIC Once the table of contents file url is identified, we can use the method `guide.pt_raw.table_of_contents.run_import_url` to import the file. To address different scenarios, multiple variants of **run_import** exist. Each of these will import into `<guide.pt_raw.locationUri>/mth=<guide.mth>/scenario=table-of-contents` directory:
# MAGIC 
# MAGIC | run_import method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_raw.table_of_contents.**run_import_url**(*url: str*) | Import a single Table-of-Contents File from Table-of-Contents url string. |
# MAGIC | guide.pt_raw.table_of_contents.**run_import_urls**(*urls: [str]*)  | Import multiple Table-of-Contents Files from python list of Table-of-Contents url strings. |
# MAGIC | guide.pt_raw.table_of_contents.**run_import_pdf**(*url_pdf: pandas.DataFrame*) | Import multiple Table-of-Contents files from pandas column of Table-of-Contents url strings. </br> If there is no `url` column, it will attempt to use the first column. |
# MAGIC | guide.pt_raw.table_of_contents.**run_import_df**(*url_pd: pyspark.sql.DataFrame*) | Import multiple Table-of-Contents files from pyspark dataframe column of Table-of-Contents url strings. </br> If there is no `url` column, it will attempt to use the first column. |
# MAGIC 
# MAGIC **NOTE**: Currently all imports are done in series. Feature development has started for concurrent file import across cluster workers.

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide(mth=202303)

# COMMAND ----------

toc_file_url = 'https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/table-of-contents/reporting_month=2023-03/2023-03-01_cigna-health-life-insurance-company_index.json?Expires=1681525425&Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cHM6Ly9kMjVrZ3o1cmlra3E0bi5jbG91ZGZyb250Lm5ldC9jb3N0X3RyYW5zcGFyZW5jeS9tcmYvdGFibGUtb2YtY29udGVudHMvcmVwb3J0aW5nX21vbnRoPTIwMjMtMDMvMjAyMy0wMy0wMV9jaWduYS1oZWFsdGgtbGlmZS1pbnN1cmFuY2UtY29tcGFueV9pbmRleC5qc29uIiwiQ29uZGl0aW9uIjp7IkRhdGVMZXNzVGhhbiI6eyJBV1M6RXBvY2hUaW1lIjoxNjgxNTI1NDI1fX19XX0_&Signature=F3MwsYMD~6jnEmwpE~bHeeWgKCQx44vPahlmHlzMlfnGO9lNYIsdCmJ4A2eqx7H~T4-QgRZqMQfQU3eZ~D7GEttumVW7LHc~QxjmEPgCanurao1CBG3KDjsIWq7wGt-CbiZD8kYZKq3gqGxvzZHPbLHhP0NvRxgFaVYVP~0t7erbE3qjey23h5NzTPsvYd8XZW9hOJOJU6AKWWSimXfoQawDhGnQjxZPw2y1yfRKrWe~Je2tNTIrF6qz5SpqIBiWstz6zR7olkS4L1wvbR4jAQWuiLRQ-1bmLtnubfqHQlae5kpDGTcQc8TAg9Wgf7UtBej5lvvUHRteVOBWdk2Kvw__&Key-Pair-Id=K1NVBEPVH9LWJP'

ptg.pt_raw.table_of_contents.run_import_url(toc_file_url)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Table of Contents Files and Meta Tables
# MAGIC 
# MAGIC We don't want to have to manage all the paths and names of all files we've ingested. So we provide a way to see all the metadata for raw files at once. Since most applications will process ingests in monthly batches, the meta data automatically filters by the `mth` used at guide instantiation. We can inspect the table of contents file we just downloaded from the dataframe, `guide.pt_raw.table_of_contents.files` and `guide.pt_raw.table_of_contents.meta` .
# MAGIC 
# MAGIC **NOTE**: The `guide.pt_raw.table_of_contents.meta` dataframe includes two additional columns; `ingested`, `reporting_entity_name`. These fields will be populated accordingly once the file is ingested and can be used as to identify raw files that have yet to be ingested.

# COMMAND ----------

display(ptg.pt_raw.table_of_contents.files)

# COMMAND ----------

display(ptg.pt_raw.table_of_contents.meta)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Ingest Streaming Job for Table of Contents files
# MAGIC 
# MAGIC While we may have the raw json files for the Table-of-Contents, it is still in a json format which is convenient for including schema and is human readible, but it isn't very performant. So we will want to ingest this json file into a more performant delta format, specifically tables: 
# MAGIC  * `pt_stage.toc_header` 
# MAGIC  * `pt_stage_reporting`.
# MAGIC 
# MAGIC The ingest job will make use of a custom streaming source, [hls-payer-mrf-sparkstreaming](https://github.com/databricks-industry-solutions/hls-price-transparency-workshop/releases/tag/v0.3.5-workshop) and write to both tables above in the a single streaming query. See [documentation](https://learn.microsoft.com/en-us/azure/databricks/libraries/workspace-libraries#--upload-a-jar-python-egg-or-python-wheel) on how to install on your cluster. It will be necessary for running ingest methods.
# MAGIC 
# MAGIC In general, the `run_ingest` methods will create a structured streaming job that will write to all target ingest tables in a single job. We offer multiple ways to run this query:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.table_of_contents.**file_ingest_query**(*file_path: str*) | Returns a **started** streaming query. This can be helpful when inspecting queries, but will continue to run (idle) after all records the file path have already been ingested. |
# MAGIC | guide.pt_stage.table_of_contents.**run_ingest_file**(*file_path: str*) | Ingest a single Table-of-Contents File from a Table-of-Contents file_path string. |
# MAGIC | guide.pt_stage.table_of_contents.**run_ingest_files**(*file_paths: [str]*)  | Ingest multiple Table-of-Contents Files from python list of Table-of-Contents file_path strings. |
# MAGIC | guide.pt_stage.table_of_contents.**run_ingest_pdf**(*file_path_pdf: pandas.DataFrame*) | Ingest multiple Table-of-Contents Files from pandas DataFrame of Table-of-Contents file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.table_of_contents.**run_ingest_df**(*file_path_pd: pyspark.sql.DataFrame*) | Ingest multiple Table-of-Contents Files from spark DataFrame of Table-of-Contents file_path strings. </br> If there is no `file_path` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.table_of_contents.**run_ingest**() | Ingests all Table-of-Contents files that have been imported, but have yet to be ingested for the `guide.mth`. |
# MAGIC 
# MAGIC **NOTE**: Target ingest tables are partitioned by `mth` and will write into the `mth` partition values assigend to `guide.mth` independent of it's actual source file path. 
# MAGIC 
# MAGIC **NOTE**: Due to the fact the some json files are single record with heavily nested entities, the convention in `spark_price_transparency` is to ingest all files such that there is a table for the json header and a table for each array element in the top level json. For Table-of-Contents there is only one array in the json top level called, `reporting_structure`, which we will write to `pt_stage.toc_reporting`.
# MAGIC 
# MAGIC **NOTE**: Even though we are ingesting into multiple tables, these tables are written from a single streaming dataframe making use of [foreachPartion](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.foreachPartition.html) with multiple [delita merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) operations.
# MAGIC 
# MAGIC **NOTE**: In development, the `run_ingest` will be able to ingest multiple files where number concurrant files will be managed as an arguement.

# COMMAND ----------

ptg.pt_stage.table_of_contents.run_ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Table of Contents Ingest Tables
# MAGIC 
# MAGIC We can now use spark sql to inspect the contents of our ingested data in `pt_stage.toc_header` and `pt_stage.toc_reporting`.
# MAGIC 
# MAGIC **NOTE**: Notice there are two entries in the header table. This is typical since heaader levels are seen at beginning and end of files with most formats placing all populated fields at the beginning of the file independent of how ordered in the 9Price Transparency Guide Schema0(https://github.com/CMSgov/price-transparency-guide).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.toc_header
# MAGIC WHERE file_name = '2023-03-01_cigna-health-life-insurance-company_index.json';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.toc_reporting
# MAGIC WHERE file_name = '2023-03-01_cigna-health-life-insurance-company_index.json';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Analytic Table Merge for Table of Contents Files
# MAGIC 
# MAGIC While it is much more performant to read the Tables of contents data in the ingest form in `pt_stage.toc_header` and `pr_stage.toc_reporting`, there are two outstanding improvements to be made:
# MAGIC  - Combine the two tables into a single table
# MAGIC  - explode columns so for a simplified schema
# MAGIC 
# MAGIC We will make these transforms and merge into our target analytic table:
# MAGIC  * `pt_stage.index_reports`
# MAGIC 
# MAGIC There are multiple method provided to run this analytics table merge, like `run_analytic`. You can see the complete list of methods below:
# MAGIC 
# MAGIC | run_ingest method | description |
# MAGIC | ----------------- | ----------- |
# MAGIC | guide.pt_stage.table_of_contents.**run_analytic_file**(*file_name: str*) | Run analytic merge from a single Table-of-Contents File from a Table-of-Contents file_name string. |
# MAGIC | guide.pt_stage.table_of_contents.**run_analytic_files**(*file_namess: [str]*)  | Run analytic merge from multiple Table-of-Contents Files from python list of Table-of-Contents file_name strings. |
# MAGIC | guide.pt_stage.table_of_contents.**run_analytic_pdf**(*file_pdf: pandas.DataFrame*) | Run analytic merge from multiple Table-of-Contents files from pandas DataFrame of Table-of-Contents file_name strings. </br> If there is no `file_name` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.table_of_contents.**run_analytic_df**(*file_pd: pyspark.sql.DataFrame*) | Run analytic merge from multiple Table-of-Contents Files from spark DataFrame of Table-of-Contents file_name strings. </br> If there is no `file_name` column, it will attempt to use the first column. |
# MAGIC | guide.pt_stage.table_of_contents.**run_analytic**() |  Run analytic merge from all files that have been ingested, but have yet to be merged into the analytic table for the `guide.mth`. |

# COMMAND ----------

ptg.pt_stage.table_of_contents.run_analytic()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Inspect Table of Contents Analytic Table
# MAGIC 
# MAGIC We now have a single high performance table that lists all the files in a table of contents source. This will be useful in our next section where we will use it to import, ingest, and merge into analytic tables a in-network-rates file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.index_reports
