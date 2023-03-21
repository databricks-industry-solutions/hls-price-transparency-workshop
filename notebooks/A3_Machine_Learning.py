# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Rate Recommender Model Concept
# MAGIC 
# MAGIC Given that we have so many provider-code rates, we intuitively think that we should be able to build a recommender system that would be able to tell what an expected rate for a given provider (provider group) should be. To make this model example consumable within a demonstration we will make the following simplifying assumptions. Some of which will fail scrutiny and require a modification for a future model build. None, the less this notebook will get us started with a first model to beat.
# MAGIC 
# MAGIC Assumptions:
# MAGIC 
# MAGIC  * Provider - Billing Code relationship can be modeled by fitting latent factors to provider and billing code vectors. These latent matrices can then be matrix multiplied to provide a matrix of all provider - billing code combinations. This is what we will use to predict what a given cost should be by provider - billing code combination. Further, we can use an existing [collaborative filtering](https://spark.apache.org/docs/2.2.0/ml-collaborative-filtering.html#collaborative-filtering-1) algorithm to fit the latent factors.
# MAGIC  * We'll exclude negotiation arrangements that are not "ffs" which will avoid the complexity of having to do fuzzy matching of difference payer coverages. Although, this model output could potentially be used as an input to predicting a bundle or capitation rate for a provider.
# MAGIC  * `billing_class` and `billing_code_modifier` will be excluded meaning that all codes will map to a single value. This will certainly result in a loss of accuracy. DS are encouraged to come up with there own segmentation of billing codes that will account for the impact of these rate attributes.
# MAGIC  * Impact due to size of plan or time of negotiation is not considered. Plan size or geographic concentration and timing can all impact negotiated rates. These factor will not be in scope for this iteration.
# MAGIC  * We can safely normalize rates such that the average rate per billing code across all obsevations is 100. This will help mitigate higher cost codes having a higher influence then lower cost codes.
# MAGIC  * These rates are not weighted, thus we consider all rates to be equal even if the provider has unequal amount instances by coverage.
# MAGIC  * Knowing that we negotiate at provider group level, we can model at the provider group level.
# MAGIC  * There is the possibility that multiple rate entries of the exact same values that come from source data. We'll drop duplicate for the model, but we would want to justify this with exploration.
# MAGIC  * We'll limit our model to only "ffs" codes that are length 5 to avoid the inclusion of prescription drugs which should perhaps be a model in itself.
# MAGIC  
# MAGIC Model Metric: 
# MAGIC 
# MAGIC  * RMSE of normalized form where normalized 100 divided by average negotiated rate.
# MAGIC  
# MAGIC  ---
# MAGIC  
# MAGIC ## Subset the data based on codes in scope
# MAGIC  
# MAGIC We'll want to identify a way that we can consistently get code in scope. To mitigate getting too many providers that don't relate to one another, we'll use the logic that we want to include any code offered by a provider that also offers code 92550 (An Audiologic Function Tests). Most likely within any Health Insurance Company that are existing subset of codes that would be used instead here.
# MAGIC 
# MAGIC **NOTE**: Normally you would exclude providers that offer too few of codes. We haven't done that here, perhaps a future source data improvement.
# MAGIC 
# MAGIC **NOTE**: Like with the Dashboards, you can run this model on deltashare data. You will have to make the table updates on the next two cells to do so. 
# MAGIC 
# MAGIC We'll likely be using these normalized rates and assigned code_ids and provider_ids for a while so we can go ahead and materialize them in these reco tables:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW reco_coverage
# MAGIC AS SELECT * FROM pt_stage.in_coverage where issuer_billing_code.code = "92550" and mth=202303;
# MAGIC SELECT * FROM reco_coverage;

# COMMAND ----------

# reco_rate_norm
from pyspark.sql.functions import col, lit, explode, count, avg, countDistinct, dense_rank, rank, length
from pyspark.sql.window import Window as W

reco_rate_norm = spark.table('pt_stage.in_rate').filter(col('mth') == lit(202303)).alias('in_rate') \
                      .join(spark.table('pt_stage.in_rate').filter(col('mth') == lit(202303)) \
                                 .join(spark.table('reco_coverage'), ['reporting_entity_name', 'sk_coverage'], 'left_semi').alias('rate_filtered'),
                            ['reporting_entity_name', 'sk_provider'], 'left_semi') \
                      .join(spark.table('pt_stage.in_coverage').filter(col('mth') == lit(202303)).filter(col('arrangement') == lit('ffs')).alias('in_coverage'), 
                            ['reporting_entity_name', 'sk_coverage'], 'left') \
                      .filter(length('in_coverage.issuer_billing_code.code') == 5) \
                      .select(col('in_coverage.issuer_billing_code.code').alias('code'),
                              col('in_rate.sk_provider').alias('sk_provider'),
                              col('in_rate.negotiated_rate').alias('negotiated_rate')).dropDuplicates() \
                      .withColumn('avg_negotiated_rate', avg('negotiated_rate').over(W.partitionBy(col('code')))) \
                      .groupBy(['code', 'avg_negotiated_rate', 'sk_provider']).agg(avg( lit(100) * col('negotiated_rate') / col('avg_negotiated_rate')).alias("rate")) \
                      .filter(col('rate').isNotNull())

reco_rate_norm.cache()

reco_code_ref = reco_rate_norm.select('code','avg_negotiated_rate').distinct() \
                              .withColumn('code_id', rank().over(W.partitionBy(lit(1)).orderBy('code')))
reco_code_ref.write.mode("overwrite").saveAsTable('pt.reco_code_ref')

reco_provider_ref = reco_rate_norm.select('sk_provider').distinct() \
                              .withColumn('provider_id', rank().over(W.partitionBy(lit(1)).orderBy('sk_provider')))
reco_provider_ref.write.mode("overwrite").saveAsTable('pt.reco_provider_ref')

reco_rate_norm.join(reco_code_ref, ['code', 'avg_negotiated_rate'], 'left') \
              .join(reco_provider_ref, ['sk_provider'], 'left') \
              .select('code','code_id','avg_negotiated_rate','sk_provider','provider_id','rate') \
              .write.mode("overwrite").saveAsTable('pt.reco_rate_norm')

reco_rate_norm.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Fit Alternating Least Squares model
# MAGIC 
# MAGIC [Collaborative filtering](https://spark.apache.org/docs/2.2.0/ml-collaborative-filtering.html#collaborative-filtering-1) is a very popular recommender system that is part of sparkML. While to most common demonstration is determining movies someone would like based upon thier input of existing movies - we'll adapt it to our objective - finding out how high a provider-rate will be negotiated based upon existing evidence. Below we have written our model to only do Test / Train evaluation and save the model to MLFlow. Tuning like cross valiadation is viable, but we'll leave that for a future discovery to keep this example concise.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
import mlflow

rank=60
maxIter=5
regParam=0.01

with mlflow.start_run() as run:
    
        rates = table('pt.reco_rate_norm')
    
        cnt_code = spark.table('pt.reco_code_ref').count()
        cnt_provider = spark.table('pt.reco_provider_ref').count()
        cnt_rate = rates.count()
        rate_density = cnt_rate/(cnt_code * cnt_provider)
    
        print(f"cnt_code: {cnt_code}")
        print(f"cnt_providere: {cnt_provider}")
        print(f"cnt_rate: {cnt_rate}")
        print(f"rate_density: {rate_density}")
        mlflow.log_metric("cnt_code", cnt_code)
        mlflow.log_metric("cnt_provider", cnt_provider)
        mlflow.log_metric("cnt_rate", cnt_rate)
        mlflow.log_metric("rate_density", rate_density)
    
        (rate_train, rate_test) = rates.randomSplit([0.8, 0.2], seed=42)
    
        mlflow.log_metric("cnt_rate_train", rate_train.count())
        mlflow.log_metric("cnt_rate_test", rate_test.count())

    
        mlflow.log_param("rank", rank)
        mlflow.log_param("maxIter", maxIter)
        mlflow.log_param("regParam", regParam)
        als = ALS(rank=rank, maxIter=maxIter, regParam=regParam, userCol="provider_id", itemCol="code_id", ratingCol="rate", coldStartStrategy="drop")
        model = als.fit(rate_train)

        rmse_eval = RegressionEvaluator(metricName="rmse", labelCol="rate", predictionCol="prediction")

        pred_test  = model.transform(rate_test)
        pred_train = model.transform(rate_train)
        rmse_test  = rmse_eval.evaluate(pred_test)
        rmse_train = rmse_eval.evaluate(pred_train)

        print(f"rmse_test: {rmse_test}")
        print(f"rmse_train: {rmse_train}")
        mlflow.log_metric("rmse_test", rmse_test)
        mlflow.log_metric("rmse_train", rmse_train)
    
        mlflow.spark.log_model(model, "model")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Inspect MLFlow Experiment
# MAGIC 
# MAGIC When this model is run, it will make an ML FLow experiments entry that is tied to this notebook. You can navigate to the results on the right side by clicking on beaker icon which will then take you to the history of experments related to this notebook. The full scope of the Databricks Machine Learning features is out of scope of this presentation, but please check out the following for more information:
# MAGIC 
# MAGIC  
# MAGIC  * [Databricks Machine Learning](https://www.databricks.com/product/machine-learning)
# MAGIC  * Th MLFlow Guide \[ [aws](https://docs.databricks.com/mlflow/index.html) \| [azure](https://learn.microsoft.com/en-us/azure/databricks/mlflow/) \| [gcp](https://docs.gcp.databricks.com/mlflow/index.html) \]
# MAGIC  * [The Big Book of MLOps](https://www.databricks.com/resources/ebook/the-big-book-of-mlops?)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Evaluate Prediction Subsample Graphically
# MAGIC 
# MAGIC All the predictions are aleady in the test and training DataFrame. Just so that we can get a sense how our forecasts are performing over rate magnitude, we'll plot our actual vs predicted for out test data...
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC Uh oh, we see negatives / close to negative values. We also see that we have some heteroscedacity at higher dollar rate amounts. In our next iteration we'll have to think of some ways to mitigate these.

# COMMAND ----------

display(pred_test.withColumn('pred', col('avg_negotiated_rate') * col('prediction') / lit(100)).filter(col('pred') <= 5000).sample(False, 0.0001, 42))
