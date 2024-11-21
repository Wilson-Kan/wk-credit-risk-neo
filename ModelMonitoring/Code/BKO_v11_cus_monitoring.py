# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta

pop_snap = "2023-10-31"
window_start = pop_snap
window_end = (datetime.strptime(pop_snap, "%Y-%m-%d") + relativedelta(years=1)).strftime("%Y-%m-%d")

# COMMAND ----------

pop_df = spark.sql(
    f"""
    select accountId, brand, score, probability
    from neo_views_credit_risk.beh_bko_score_test as a
    where monthend = '{pop_snap}'
  """
)

target_df = spark.sql(
    f"""
    select distinct accountId, 1 as co
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{window_start}' and '{window_end}'
    and chargedOffReason in   ("CONSUMER_PROPOSALS",
    "BANKRUPTCY",
    "CREDIT_COUNSELLING_SOLUTIONS",
    "SETTLEMENTS")
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

monitor_df = monitor_df.withColumn(
    "is_co", 
    when(col("co") == 1, 1).otherwise(0)
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
  plt.plot(fpr,tpr,label="auc="+str(auc))
  plt.legend(loc=4)
  plt.title(title)
  plt.show()

# COMMAND ----------

plot_roc(score_this, 'probability', 'is_co', 'ROC curve - BKO behavioural')
