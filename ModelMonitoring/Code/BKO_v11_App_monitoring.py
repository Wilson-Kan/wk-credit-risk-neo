# Databricks notebook source
from datetime import datetime

pop_start = "2024-01-01"
pop_end = "2024-05-31"
window_start = pop_start
window_end = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

pop_df = spark.sql(
    f"""
    select a.applicationId, a.score, a.probability, b.applicationDecision
    from neo_data_science_production.credit_risk_application_bko_scores as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationDate between '{pop_start}' and '{pop_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = spark.sql(
    f"""
    select distinct applicationId, chargedOffReason
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{window_start}' and '{window_end}'
    and chargedOffReason in   ("CONSUMER_PROPOSALS",
    "BANKRUPTCY",
    "CREDIT_COUNSELLING_SOLUTIONS",
    "SETTLEMENTS")
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")

# COMMAND ----------

from pyspark.sql.functions import when, col

monitor_df = monitor_df.withColumn("is_cp", when(col("chargedOffReason")=="CONSUMER_PROPOSALS", 1).otherwise(0)).withColumn("is_bk", when(col("chargedOffReason")=="BANKRUPTCY", 1).otherwise(0)).withColumn("is_ccs", when(col("chargedOffReason")=="CREDIT_COUNSELLING_SOLUTIONS", 1).otherwise(0))

# COMMAND ----------

monitor_df = monitor_df.withColumn(
    "is_accel_co", 
    when(col("chargedOffReason").isNotNull(), 1).otherwise(0)
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

plot_roc(score_this, 'probability', 'is_accel_co', 'ROC curve - Accelerated')

# COMMAND ----------

plot_roc(score_this, 'probability', 'is_cp', 'ROC curve - Consumer Proposal')

# COMMAND ----------

plot_roc(score_this, 'probability', 'is_bk', 'ROC curve - Bankruptcy')

# COMMAND ----------

plot_roc(score_this, 'probability', 'is_ccs', 'ROC curve - Credit Counciling Solution')

# COMMAND ----------

score_no999 = score_this.filter(col('score') < 999)

# COMMAND ----------

plot_roc(score_no999, 'probability', 'is_accel_co', 'ROC curve - Accelerated no 999')

# COMMAND ----------

plot_roc(score_no999, 'probability', 'is_cp', 'ROC curve - Consumer Proposal')

# COMMAND ----------

plot_roc(score_no999, 'probability', 'is_bk', 'ROC curve - Bankruptcy')

# COMMAND ----------

plot_roc(score_no999, 'probability', 'is_ccs', 'ROC curve - Credit Counciling Solution no 999')

# COMMAND ----------


