# Databricks notebook source
# MAGIC %md 
# MAGIC # UHG Table-of-Contents
# MAGIC 
# MAGIC Multiple payers were included in the delta share so attendees can see how all data is standardized in the analytic layer eventhough payers use different plan organization and different locations for provider details. This code is provided with minimal comment, but can still be a useful reference.

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide(mth=202303)

# NOTE: UHG maintains many, but smaller TOCs by plan
uhg_aa_toc_url = "https://uhc-tic-mrf.azureedge.net/public-mrf/2023-03-01/2023-03-01_American-Airlines_index.json"

ptg.pt_raw.table_of_contents.run_import_url(uhg_aa_toc_url)

# COMMAND ----------

ptg.pt_stage.table_of_contents.run_ingest()
# display(spark.sql("SELECT * FROM pt_stage.toc_header WHERE file_name = '2023-03-01_American-Airlines_index.json'"))
# display(spark.sql("SELECT * FROM pt_stage.toc_reporting WHERE file_name = '2023-03-01_American-Airlines_index.json'"))

# COMMAND ----------

ptg.pt_stage.table_of_contents.run_analytic()
# display(spark.sql("SELECT * FROM pt_stage.index_reports WHERE file_name = '2023-03-01_American-Airlines_index.json'"))

# COMMAND ----------

# MAGIC %md
# MAGIC # UHG In-Network-Rates
# MAGIC 
# MAGIC We are going to pick out just one of AAs plans, **PS1-50_C2**.

# COMMAND ----------

from pyspark.sql.functions import col, lit, explode

url_aa_plan = spark.table('pt_stage.index_reports') \
                               .filter(col('file_type') == lit('in-network-rates')) \
                               .filter(col('file_name') == lit('2023-03-01_American-Airlines_index.json')) \
                               .filter(col('location').contains('PS1-50_C2')) \
                               .withColumnRenamed('location', 'url')
# display(url_aa_plan)
ptg.pt_raw.in_network_rates.run_import_df(url_aa_plan)

# COMMAND ----------

aa_file = ptg.pt_raw.in_network_rates.files.filter(col('file_name').contains('United-HealthCare-Services'))
ptg.pt_stage.in_network_rates.run_ingest_df(aa_file)

# COMMAND ----------

ptg.pt_stage.in_network_rates.run_analytic_df(aa_file)

# COMMAND ----------

# MAGIC %md 
# MAGIC # UHG Provider-Reference
# MAGIC 
# MAGIC UHG uses provider group ids, but doesn't use remote files for provider reference. Thue we do not need to run any provider reference methods:
# MAGIC  - `pt_stage.in_pr_loc` will have no records
# MAGIC  - `pt_stage.in_provider` will be populated with sk_provider and provider groups
# MAGIC  - `pt_stage.in_rate` will have `sk_provider` already populated, no need to run anything for UHG in Provider-Reference.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_pr_loc WHERE reporting_entity_name = "United HealthCare Services, Inc.";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_provider WHERE reporting_entity_name = "United HealthCare Services, Inc." LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pt_stage.in_rate WHERE reporting_entity_name = "United HealthCare Services, Inc." LIMIT 2;

# COMMAND ----------

# MAGIC %md
# MAGIC # UHG Allowed-Amounts

# COMMAND ----------

from pyspark.sql.functions import col, lit, explode

url_aa_plan = spark.table('pt_stage.index_reports') \
                               .filter(col('file_type') == lit('allowed-amounts')) \
                               .filter(col('location').contains('American-Airlines_PREFERRED')) \
                               .withColumnRenamed('location', 'url')

# display(url_aa_plan)
ptg.pt_raw.allowed_amounts.run_import_df(url_aa_plan)

# COMMAND ----------

ptg.pt_stage.allowed_amounts.run_ingest()

# COMMAND ----------

ptg.pt_stage.allowed_amounts.run_analytic()
