# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Anthem Workflow
# MAGIC 
# MAGIC Multiple payers were included in the delta share so attendees can see how all data is standardized in the analytic layer eventhough payers use different plan organization and different locations for provider details. This code is provided with minimal comment, but can still be a useful reference.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Anthem Table-of-Contents

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide(mth=202303)

antm_toc_url = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2023-03-01_anthem_index.json.gz"

ptg.pt_raw.table_of_contents.run_import_url(antm_toc_url)

# COMMAND ----------

ptg.pt_stage.table_of_contents.run_ingest()

# COMMAND ----------

ptg.pt_stage.table_of_contents.run_analytic()

# COMMAND ----------

# MAGIC %md
# MAGIC # Anthem In-Network-Rates
# MAGIC 
# MAGIC Anthem has invalid file nme conventions causing name collisions.

# COMMAND ----------

url_antm_plan = spark.table('pt_stage.index_reports') \
                               .filter(col('file_type') == lit('in-network-rates')) \
                               .filter(col('reporting_entity_name') == lit('Anthem Inc')) \
                               .withColumn('reporting_plan', explode(col("reporting_plans"))) \
                               .filter(col('reporting_plan.plan_name') == lit('PREFERRED BLUE PPO - OMNI INTERNATIONAL CORP - ANTHEM')) \
                               .filter(col('location').contains('2023-03_430_65B0_in-network-rates.json.gz')) \
                               .withColumnRenamed('location', 'url')
# display(url_antm_plan)
ptg.pt_raw.in_network_rates.run_import_df(url_antm_plan)

# COMMAND ----------

ptg.pt_stage.in_network_rates.run_ingest_file("dbfs:/user/hive/warehouse/pt_raw.db/_raw/mth=202303/schema=in-network-rates/2023-03_430_65B0_in-network-rates.json")

# COMMAND ----------

ptg.pt_stage.in_network_rates.run_analytic()

# COMMAND ----------

# MAGIC %md
# MAGIC # Anthem Provider-Reference
# MAGIC 
# MAGIC Isn't need since Anthem doesn't use provider reference locations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Anthem Allowed Amounts
# MAGIC 
# MAGIC Didn't find any allowed amounts for anthem in the anthem toc plan. This can happen when the In-Network-Rates and Allowed-Amounts are in single plan per file.

# COMMAND ----------

url_antm_plan = spark.table('pt_stage.index_reports') \
                               .filter(col('file_type') == lit('allowed-amounts')) \
                               .filter(col('reporting_entity_name') != lit('Anthem Inc')) \
                               .filter(col('location').isNotNull()) \
                               .withColumnRenamed('location', 'url').limit(1)
display(url_antm_plan)
