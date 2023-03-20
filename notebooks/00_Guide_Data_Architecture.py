# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Object Model and Data Architecture
# MAGIC 
# MAGIC Before we start processing price tranparency files, we must first establish the conventions that we'll use for our Price Transparency application and specify all entities that will be in scope.
# MAGIC 
# MAGIC All of the methods and visuals used in the workshop notebooks require the python library [spark-price-transparency](https://pypi.org/project/spark-price-transparency/0.1.85/). The streaming ingest methods require [payer-mrf-streamsource-0.3.5.jar
# MAGIC ](https://github.com/databricks-industry-solutions/hls-payer-mrf-sparkstreaming/releases/tag/0.3.5v) Please see the workshop [readme](https://github.com/databricks-industry-solutions/hls-price-transparency-workshop#cluster-dependencies) for details install instructions.
# MAGIC 
# MAGIC 
# MAGIC ## Curation Convention 
# MAGIC 
# MAGIC We will adopt [medalion architecture](https://www.databricks.com/glossary/medallion-architecture) as a convention for price transparency. Specifically , we will generalize the curation across three curations layers, each with a deticated database:
# MAGIC 
# MAGIC | Medallion Databases | Description | Location Convention|
# MAGIC | -------------- | ----------- | -------- |
# MAGIC | pt_raw (bronze)        | Landing zone for raw data, intended to source data in original form there are partition folders </br> by month and schema | .../pt_raw.db/\_raw/mth=**\<YYYYMM\>**/schema=**\<schema\>**/ </br> where **\<schema\>** is `table-of-contents`, `in-network-rates`, `provider-reference`, or `allowed-amounts` |
# MAGIC | pt_stage (silver)      | Stored in delta, there are two curation layers within pt_stage:  </br> **ingest**: raw json converted to complex types with arrays exploded into rows </br> **analytic**: enriched, semi-normalized form of **ingest** data for analytics | All tables, both **ingest** and **analytic** will be in  .../pt_stage.db/ |
# MAGIC | pt (gold)              | The gold layer should include atleast the analytic tables as views from staging. The database creation, views creation, or any other buisiness analytics tables are not provided since they will be specific to individual customer use case and should include insights / intellectual property specific to businesses. The gold layer is useful because it provides a clear separation of what is provided by the spark-price-transparency accelerator and what business value is brought by the development team and business. | Customer should manually create views from silver analytic tables. | 
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Curation Entities
# MAGIC 
# MAGIC So we have the curation conventions which includes entities, but we want to know all the entities invovled when applying the above conventions to the [Price Transparency Guide Schemas](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas) and why we would organize these entities. Below you will see all entities in the project. Before each of the entity types you'll the method call used for populating these entities with data. The astricks asterisk indicates that the method is part of a parent class which is schema - the rest of the notebooks in this guide will detail each of the schems and each will follow this same entity convention. You can see across all schemas our data workflow will take on the following pattern:
# MAGIC  * **Run Import** - This method will take url(s) of files that need to be downloaded and save them in the **pt_raw** database location
# MAGIC  * **Import Files** - These files are all in json format that doesn't have a serde to put a schema on and register in our metastore. Thus, you can only ingest or inspect metadata on these files.
# MAGIC  * **Run Ingest** - This method will stream the Import files into their respective schema Ingest Tables which are in delt format. 
# MAGIC  * **Ingest Tables** - All Tables have schemas that match the [CMS Price Transparency Guide](https://github.com/CMSgov/price-transparency-guide) are preserved, but with array elements broken into separare tables and stored as delta for better performance.
# MAGIC  * **Run Analytic** - This method runs a batch job with Ingest Tables source to populate Analytic tables.
# MAGIC  * **Analytic Tables** - These tables are a semi-normalized form of the original schema intended to be shared with business users as base tables for insights.

# COMMAND ----------

# DBTITLE 1,Install graphics for visualizations
# MAGIC %sh
# MAGIC apt-get install -y graphviz
# MAGIC pip install graphviz

# COMMAND ----------

from spark_price_transparency.entity_viz import get_entity_html
displayHTML(get_entity_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Workflow Lineage
# MAGIC 
# MAGIC The entities graphic shows how each type group interacts with one another. You can use the graphic below to see each of the specific dependencies within each of the Price Transparency schemas. You will notice that the lineage for Table-of-Contents Schema and Allowed-Amount Schema are independent of other schemas while In-Network-Rates Schema and Provider-Reference Schema are combined. This combination is a consequence of the CMS Price-Transparency-Guide allowing providers data to be provided in separate Provider-Reference Files via location provided in th In-Network-Rates Files. Not all reportinging entity plans make use of this schema option.

# COMMAND ----------

from spark_price_transparency.curate_viz import get_curate_html
displayHTML(get_curate_html())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Transparency Initialization (Entity Creation)
# MAGIC 
# MAGIC The raw directory and pt_stage database and tables can be created using the `guide.initialize_pt` method. This only needs to be run once in your chosen workspace.
# MAGIC 
# MAGIC **NOTE** - Currently the solution defaults to using the workspace `hive_metastore`. Future feature will allow for specifying a catalog.
# MAGIC 
# MAGIC **NOTE** - After you run `guide.initialize` you are able to inspect all of the `pt_stage` tables by clicking on the icons above.

# COMMAND ----------

from spark_price_transparency.guide import Guide
ptg = Guide(mth=202303)
ptg.initialize()
display(spark.sql("SHOW TABLES IN hive_metastore.pt_stage"))
