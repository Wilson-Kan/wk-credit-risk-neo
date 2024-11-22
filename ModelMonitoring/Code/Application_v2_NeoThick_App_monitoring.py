# Databricks notebook source
from datetime import datetime

pop_start = "2023-08-01"
pop_end = "2024-10-31"
window_start = pop_start
window_end = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

pop_df = spark.sql(
    f"""
    select a.applicationId, a.raw_pred, b.applicationDecision
    from neo_views_credit_risk.wk_neo_thick_app_v2 as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationCompletedAt_mt between '{pop_start}' and '{pop_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = spark.sql(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{window_start}' and '{window_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")

# COMMAND ----------

from pyspark.sql.functions import when, col

monitor_df = monitor_df.withColumn(
    "is_def", 
    when(col("is_def").isNotNull(), 1).otherwise(0)
)

# COMMAND ----------

score_this = monitor_df.toPandas()

# COMMAND ----------

from sklearn import metrics
import matplotlib.pyplot as plt

def plot_roc(df, y_pred, y_target, title):
  y_pred_proba = df[y_pred]
  y = df[y_target]
  fpr, tpr, _ = metrics.roc_curve(y,  y_pred_proba)
  auc = metrics.roc_auc_score(y, y_pred_proba)
  plt.plot(fpr,tpr,label="data 1, auc="+str(auc))
  plt.legend(loc=4)
  plt.title(title)
  plt.show()

# COMMAND ----------

plot_roc(score_this, 'raw_pred', 'is_def', 'ROC curve - App Neo Thick')

# COMMAND ----------


