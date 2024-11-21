# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import col, when

pop_start = "2024-06-01"
pop_end = "2024-08-31"
window_start = pop_start
window_end = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

pop_df = spark.sql(
    f"""
    select a._id as applicationId, b.machineLearningRiskCheckResult.probabilityOfFraud as bunsen_score
    from neo_raw_production.credit_onboarding_service_credit_applications as a
    inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as b
    on a.userReportsMetadataSnapshotId = b._id
    where a.completedAt between '{pop_start}' and '{pop_end}'
    and a.decision = 'APPROVED'
    and b.machineLearningRiskCheckResult.probabilityOfFraud is not null
  """
)

target_df = spark.sql(
    f"""
    select distinct applicationId, 1 as is_fraud
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{window_start}' and '{window_end}'
    and chargeOffCategory in   ("FRAUD")
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")

# COMMAND ----------

monitor_df = monitor_df.withColumn(
    "is_fraud", 
    when(col("is_fraud").isNotNull(), 1).otherwise(0)
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

plot_roc(score_this, 'bunsen_score', 'is_fraud', 'ROC curve - Bunsen')
