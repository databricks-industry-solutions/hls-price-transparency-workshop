# Databricks notebook source
# MAGIC %md # Dashboards
# MAGIC 
# MAGIC Dashboards allow you to publish graphs and visualizations and share them in a presentation format with your organization. This notebook shows how to create, and edit a dashboard with Price Transparency Data.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Dashboards are composed of elements. These elements are created by output from notebook cells. We'll create some price transparency elements / cells for the dashboard we're going to be building. The first cell creates a dashboard title using the ``displayHTML()`` function.

# COMMAND ----------

displayHTML("""<font size="6" color="red" face="sans-serif">Price Transparency Setting-Class Rates</font>""")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Create Dashboard `PT_rates`
# MAGIC 
# MAGIC We can start our dashboard from just the cell above! Go into the cell above, in the top right corner you will see a bar chart icon. Click on that icon and add our header markdown to a new dashboard called, `PT_rates`. This will open a new tab for you where you can organize your dashboard. However, let's keep going in this notebook and see how to add a few more items.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Temp Table for graphs
# MAGIC 
# MAGIC We'll want to have some source data with which to make some charts. Let's write that now for two arbitrary codes; 33361 and 15572. We'll use the `in_coverage` table to filter by these values and join to `in_rate` table to get these rates for all providers in network. We'll save this temp table as `pt_setting_cost`. Notice, we created a custom column to identify if this is hospital or out patient as well as the class, professional or institutional.
# MAGIC 
# MAGIC **NOTE**: Unlike the header markdown above, you add table and graph in the output. Not a very common when you have a lot of records so we won't do it here. 
# MAGIC 
# MAGIC **NOTE**: The create temp view query below assumes that you have already run all of the Spark Price Transparency code and created the tables. If you have not, you can run the delta share configuration in [A1_Delta_Share](./A1_Delta_Share) and update the code below to evaluate Dashboards using delta share data. Yes! You can build dashboard directly from a Delta Share source.

# COMMAND ----------

use_delta_share = False
if use_delta_share:
  spark.read.format("deltaSharing").load('dbfs:/tmp/pt_config.share#price-transparency-workshop.pt_stage.in_coverage').createOrReplaceTempView('in_coverage')
  spark.read.format("deltaSharing").load('dbfs:/tmp/pt_config.share#price-transparency-workshop.pt_stage.in_rate').createOrReplaceTempView('in_rate')
else:
  spark.table('pt_stage.in_coverage').createOrReplaceTempView('in_coverage')
  spark.table('pt_stage.in_rate').createOrReplaceTempView('in_rate')


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pt_setting_cost
# MAGIC AS SELECT
# MAGIC    CONCAT_WS('_', IF(ARRAYS_OVERLAP(service_code, ARRAY("21", "22", "23")), "hospital", "outpatient"), billing_class) setting_class,
# MAGIC    billing_code,
# MAGIC    negotiated_rate
# MAGIC FROM
# MAGIC     in_rate
# MAGIC RIGHT JOIN
# MAGIC     (SELECT sk_coverage, issuer_billing_code.code billing_code FROM in_coverage 
# MAGIC      WHERE reporting_entity_name = "Cigna Health Life Insurance Company" AND ARRAY_CONTAINS(ARRAY("15572", "33361"), issuer_billing_code.code)) c
# MAGIC ON
# MAGIC     in_rate.sk_coverage = c.sk_coverage
# MAGIC WHERE
# MAGIC     billing_code_modifier = ARRAY("");
# MAGIC SELECT * FROM pt_setting_cost;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create some dashboard graphs
# MAGIC 
# MAGIC These graphs are simple queries off the temp table, `pt_setting_cost`, we created. They are all pre-configured, you just need to go into the bar chart icon in the top right corner of each and add them to your `PT_rates` dashboard.

# COMMAND ----------

# DBTITLE 1,Billing Code 15572 Setting & Class
display(spark.sql("SELECT * FROM pt_setting_cost WHERE billing_code='15572'"))

# COMMAND ----------

# DBTITLE 1,Billing Code 33361 Setting & Class
display(spark.sql("SELECT * FROM pt_setting_cost WHERE billing_code='33361'"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add HTML graphics, ledgends, or comments
# MAGIC 
# MAGIC A lot of times you will need branding, some hyperlinks or explaination. You can use html for this. Let go ahead and run one of the price transparency graphics and add to our dashboard. Same as the header markdown above, we'll add the graphic using the bar graph icon in the top right corner.

# COMMAND ----------

from spark_price_transparency.end2end_viz import get_end2end_html
displayHTML(get_end2end_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Inspect the `PT_Rates` dashboard
# MAGIC 
# MAGIC So we've added all our graphics, let go plan out our dashboard. In the workbook menu, go to View -> Dashboards -> PT_rates. Alternately, you should be able to also inspect from the window that automatically opened when we created our dashboard `PT_rates`.
