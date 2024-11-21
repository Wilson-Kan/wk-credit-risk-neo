# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta

pop_snap = "2023-10-31"
window_start = pop_snap
window_end = (datetime.strptime(pop_snap, "%Y-%m-%d") + relativedelta(years=1)).strftime("%Y-%m-%d")

# COMMAND ----------

pop_df = spark.sql(
    f"""
    select accountId, brand, bad_rate, modelScore
    from neo_data_science_production.credit_risk_behavior_pd_v2_1 as a
    where referenceDate = '{pop_snap}'
  """
)

target_df = spark.sql(
    f"""
    select distinct accountId, 1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{window_start}' and '{window_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")

# COMMAND ----------

from pyspark.sql.functions import when, col

monitor_df = monitor_df.withColumn("is_def", when(col("is_def")==1, 1).otherwise(0))

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
  plt.plot(fpr,tpr,label="auc="+str(auc))
  plt.legend(loc=4)
  plt.title(title)
  plt.show()

# COMMAND ----------

plot_roc(score_this, 'bad_rate', 'is_def', 'ROC curve - BEH')
