# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from sklearn import metrics
import matplotlib.pyplot as plt
from pyspark.sql.functions import when, col, datediff
from scipy.stats import ks_2samp

monitoring_res = pd.DataFrame()
grid_res = pd.DataFrame()
psi_res = pd.DataFrame()
csi_res = pd.DataFrame()

# COMMAND ----------

# MAGIC %md ## Dates

# COMMAND ----------

generic_app_start = "2023-08-01"
generic_app_end = "2023-10-31"
generic_app_tar_start = "2023-08-01"
generic_app_psi_start = "2024-08-01"
generic_app_tar_end = "2024-10-31"

generic_cus_start = "2023-10-31"
generic_cus_tar_start = "2023-10-31"
generic_cus_tar_end = "2024-10-31"

generic_psi_mmyy_t0 = [2023, 10]
generic_psi_mmyy_t1 = [2024, 10]

oscar_start = "2024-01-31"
oscar_tar_start = "2024-01-31"
oscar_tar_end = "2025-01-31"

# COMMAND ----------

# MAGIC %md ## Functions

# COMMAND ----------

def data_pull(query):
    return spark.sql(query)

def auc_calc(df, y_pred, y_target):
    y_pred_proba = df[y_pred]
    y = df[y_target]
    return metrics.roc_auc_score(y, y_pred_proba)

def ks_calc(df, y_pred, y_target):
    return ks_2samp(df.loc[df[y_target]==0,y_pred], df.loc[df[y_target]==1,y_pred])[0]

def by_segment(segment, df, y_pred, y_target):
    res = []
    for s in df[segment].unique():
        temp_df = df[df[segment] == s]
        auc = auc_calc(temp_df, y_pred, y_target)
        gini = 2 * (auc - 1)
        ks = ks_calc(temp_df, y_pred, y_target)
        res.append((s, gini, ks, auc))
    return res

def grid(df, model_name, y_pred, y_target, date_target, y_date, bin_num=10):
    df["decile"] = pd.qcut(df[y_pred], q=bin_num, duplicates="drop")
    result = grid_this.groupby("decile").agg(
        count=(y_pred, "count"),
        target=(y_target, "sum"),
        avg_datediff=(date_target, "mean"),
    )
    result["rate"] = result["target"] / result["count"]
    result["avg_month_to_def"] = result["avg_datediff"] / 30
    result.reset_index(inplace=True)
    result["model"] = model_name
    result["Pop Date"] = y_date
    result["Executed"] = datetime.today()
    result = result[
        ["model", "decile", "rate", "avg_month_to_def", "count", "Pop Date", "Executed"]
    ]
    return result


def psi(df, y, tdim, t_0, t_1, bin_num=5):
    t0_df = df[df[tdim] == t_0][y]
    t1_df = df[df[tdim] == t_1][y]
    [t0_cut, bins] = pd.qcut(
        t0_df, q=bin_num, retbins=True, labels=False, duplicates="drop"
    )
    t1_cut = pd.cut(t1_df, bins=bins, labels=False)
    c0 = (t0_cut.groupby(t0_cut).count() / t0_cut.count()).to_frame()
    c0.columns = ["t0"]
    c1 = (t1_df.groupby(t1_df).count() / t1_df.count()).to_frame()
    c1.columns = ["t1"]
    res = c0.join(c1)
    res["sum_this"] = (res["t0"] - res["t1"]) * np.log(res["t0"] / res["t1"])
    return res["sum_this"].sum()

# COMMAND ----------

# MAGIC %md ## Subprime V1

# COMMAND ----------

# Subprime V1 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.score, a.probability, b.applicationDecision, b.brandName as brand
    from neo_data_science_production.credit_risk_application_pd_subprime_scores as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationDate between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["Subprime V1"],
    "Pop Start Date": [generic_app_start],
    "Pop End Date": [generic_app_end],
    "Window Start Date": [generic_app_tar_start],
    "Window End Date": [generic_app_tar_end],
    "GINI": [2*(auc_calc(score_this, "probability", "is_def")-0.5)],
    "KS": [ks_calc(score_this, "probability", "is_def")],
    "AUC": [auc_calc(score_this, "probability", "is_def")],
    "Executed": [datetime.today()],
}
score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]
seg_calc = by_segment("brand", score_this, "probability", "is_def")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"Subprime V1 - {n}")
    temp_df["Pop Start Date"].append(generic_app_start)
    temp_df["Pop End Date"].append(generic_app_end)
    temp_df["Window Start Date"].append(generic_app_tar_start)
    temp_df["Window End Date"].append(generic_app_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Thick V1

# COMMAND ----------

# Thick V1 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.score, a.probability, b.applicationDecision, b.brandName as brand
    from neo_data_science_production.credit_risk_application_pd_thick_scores as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationDate between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["Thick V1"],
    "Pop Start Date": [generic_app_start],
    "Pop End Date": [generic_app_end],
    "Window Start Date": [generic_app_tar_start],
    "Window End Date": [generic_app_tar_end],
    "GINI": [2*(auc_calc(score_this, "probability", "is_def")-0.5)],
    "KS": [ks_calc(score_this, "probability", "is_def")],
    "AUC": [auc_calc(score_this, "probability", "is_def")],
    "Executed": [datetime.today()],
}
score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]
seg_calc = by_segment("brand", score_this, "probability", "is_def")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"Thick V1 - {n}")
    temp_df["Pop Start Date"].append(generic_app_start)
    temp_df["Pop End Date"].append(generic_app_end)
    temp_df["Window Start Date"].append(generic_app_tar_start)
    temp_df["Window End Date"].append(generic_app_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Thin V1

# COMMAND ----------

# Thin V1 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.score, a.probability, b.applicationDecision, b.brandName as brand
    from neo_data_science_production.credit_risk_application_pd_thin_scores as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationDate between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["Thin V1"],
    "Pop Start Date": [generic_app_start],
    "Pop End Date": [generic_app_end],
    "Window Start Date": [generic_app_tar_start],
    "Window End Date": [generic_app_tar_end],
    "GINI": [2*(auc_calc(score_this, "probability", "is_def")-0.5)],
    "KS": [ks_calc(score_this, "probability", "is_def")],
    "AUC": [auc_calc(score_this, "probability", "is_def")],
    "Executed": [datetime.today()],
}
score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]
seg_calc = by_segment("brand", score_this, "probability", "is_def")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"Thin V1 - {n}")
    temp_df["Pop Start Date"].append(generic_app_start)
    temp_df["Pop End Date"].append(generic_app_end)
    temp_df["Window Start Date"].append(generic_app_tar_start)
    temp_df["Window End Date"].append(generic_app_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## BKO V1.1

# COMMAND ----------

# BKO V1.1 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.score, a.probability, b.applicationDecision, b.brandName as brand
    from neo_data_science_production.credit_risk_application_bko_scores as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationDate between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId, chargedOffReason
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and chargedOffReason in   ("CONSUMER_PROPOSALS",
    "BANKRUPTCY",
    "CREDIT_COUNSELLING_SOLUTIONS",
    "SETTLEMENTS")
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_accel_co", when(col("chargedOffReason").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["BKO V1.1"],
    "Pop Start Date": [generic_app_start],
    "Pop End Date": [generic_app_end],
    "Window Start Date": [generic_app_tar_start],
    "Window End Date": [generic_app_tar_end],
    "GINI": [2*(auc_calc(score_this, "probability", "is_accel_co")-0.5)],
    "KS": [ks_calc(score_this, "probability", "is_accel_co")],
    "AUC": [auc_calc(score_this, "probability", "is_accel_co")],
    "Executed": [datetime.today()],
}
score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]
seg_calc = by_segment("brand", score_this, "probability", "is_accel_co")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"BKO V1.1 - {n}")
    temp_df["Pop Start Date"].append(generic_app_start)
    temp_df["Pop End Date"].append(generic_app_end)
    temp_df["Window Start Date"].append(generic_app_tar_start)
    temp_df["Window End Date"].append(generic_app_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Neo Subprime V2

# COMMAND ----------

# Neo Subprime V2 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.raw_pred, b.applicationDecision
    from neo_views_credit_risk.wk_neo_subprime_app_v2 as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationCompletedAt_mt between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Neo Subprime V2"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "raw_pred", "is_def")-0.5)],
        "KS": [ks_calc(score_this, "raw_pred", "is_def")],
        "AUC": [auc_calc(score_this, "raw_pred", "is_def")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Neo Thick V2

# COMMAND ----------

# Neo Thick V2 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.raw_pred, b.applicationDecision
    from neo_views_credit_risk.wk_neo_thick_app_v2 as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationCompletedAt_mt between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Neo Thick V2"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "raw_pred", "is_def")-0.5)],
        "KS": [ks_calc(score_this, "raw_pred", "is_def")],
        "AUC": [auc_calc(score_this, "raw_pred", "is_def")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Neo Thin V2

# COMMAND ----------

# Neo Thin V2 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.raw_pred, b.applicationDecision
    from neo_views_credit_risk.wk_neo_thin_app_v2 as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationCompletedAt_mt between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Neo Thin V2"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "raw_pred", "is_def")-0.5)],
        "KS": [ks_calc(score_this, "raw_pred", "is_def")],
        "AUC": [auc_calc(score_this, "raw_pred", "is_def")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Tims Thick V2

# COMMAND ----------

# Tims Thick V2 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.raw_pred, b.applicationDecision
    from neo_views_credit_risk.wk_tims_thick_app_v2 as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationCompletedAt_mt between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Tims Thick V2"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "raw_pred", "is_def")-0.5)],
        "KS": [ks_calc(score_this, "raw_pred", "is_def")],
        "AUC": [auc_calc(score_this, "raw_pred", "is_def")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Tims Thin V2

# COMMAND ----------

# Tims Thin V2 application
pop_df = data_pull(
    f"""
    select a.applicationId, a.raw_pred, b.applicationDecision
    from neo_views_credit_risk.wk_tims_thin_app_v2 as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationCompletedAt_mt between '{generic_app_start}' and '{generic_app_end}'
    and applicationDecision = 'APPROVED'
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId,  1 as is_def
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_def", when(col("is_def").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Tims Thin V2"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "raw_pred", "is_def")-0.5)],
        "KS": [ks_calc(score_this, "raw_pred", "is_def")],
        "AUC": [auc_calc(score_this, "raw_pred", "is_def")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Beaker

# COMMAND ----------

# Beaker V1 application (HBC in store Fraud)
# Current scoring in: neo_raw_production.application_fraud_service_fraud_evaluations WHERE machineLearningData.modelOutput.model_name
pop_df = data_pull(
    f"""
    select applicationId, probChallenger as beaker_score
    from neo_data_science_historical.beaker_champion_challenger
    where applicationDate between '{datetime.strptime(generic_app_start, '%Y-%m-%d')}' and '{datetime.strptime(generic_app_end, '%Y-%m-%d')}'
    and probChallenger is not null
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId, 1 as is_fraud
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and chargeOffCategory in ("FRAUD")
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_fraud", when(col("is_fraud").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Beaker (HBC in store Fraud)"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "beaker_score", "is_fraud")-0.5)],
        "KS": [ks_calc(score_this, "beaker_score", "is_fraud")],
        "AUC": [auc_calc(score_this, "beaker_score", "is_fraud")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Bunsen

# COMMAND ----------

# Bunsen V1 application (online Fraud)
# Current scoring in: neo_raw_production.application_fraud_service_fraud_evaluations WHERE machineLearningData.modelOutput.model_name
pop_df = data_pull(
    f"""
    select applicationId, pFraud as bunsen_score
    from neo_data_science_historical.bunsen_1_1_historical_daily
    where applicationDate between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and pFraud is not null
  """
)

target_df = data_pull(
    f"""
    select distinct applicationId, 1 as is_fraud
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{generic_app_tar_start}' and '{generic_app_tar_end}'
    and chargeOffCategory in ("FRAUD")
  """
)

monitor_df = pop_df.join(target_df, on="applicationId", how="left")
monitor_df = monitor_df.withColumn(
    "is_fraud", when(col("is_fraud").isNotNull(), 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = pd.DataFrame(
    {
        "Model": ["Bunsen (Online Fraud)"],
        "Pop Start Date": [generic_app_start],
        "Pop End Date": [generic_app_end],
        "Window Start Date": [generic_app_tar_start],
        "Window End Date": [generic_app_tar_end],
        "GINI": [2*(auc_calc(score_this, "bunsen_score", "is_fraud")-0.5)],
        "KS": [ks_calc(score_this, "bunsen_score", "is_fraud")],
        "AUC": [auc_calc(score_this, "bunsen_score", "is_fraud")],
        "Executed": [datetime.today()],
    }
)
monitoring_res = pd.concat([monitoring_res, temp_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## BEH V2.1

# COMMAND ----------

# Behavioural PD V2.1 AUC
pop_df = data_pull(
    f"""
    select accountId, brand, bad_rate, modelScore
    from neo_data_science_production.credit_risk_behavior_pd_v2_1 as a
    where referenceDate = '{generic_cus_start}'
  """
)

target_df = data_pull(
    f"""
    select accountId, 1 as is_def, min(referenceDate) as def_dt
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_cus_tar_start}' and '{generic_cus_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
    group by accountId
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")
monitor_df = monitor_df.withColumn("is_def", when(col("is_def") == 1, 1).otherwise(0))
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["Behavioural PD V2.1"],
    "Pop Start Date": [generic_cus_start],
    "Pop End Date": [None],
    "Window Start Date": [generic_cus_tar_start],
    "Window End Date": [generic_cus_tar_end],
    "GINI": [2*(auc_calc(score_this, "bad_rate", "is_def")-0.5)],
    "KS": [ks_calc(score_this, "bad_rate", "is_def")],
    "AUC": [auc_calc(score_this, "bad_rate", "is_def")],
    "Executed": [datetime.today()],
}

score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]

seg_calc = by_segment("brand", score_this, "bad_rate", "is_def")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"Behavioural PD V2.1 - {n}")
    temp_df["Pop Start Date"].append(generic_cus_start)
    temp_df["Pop End Date"].append(None)
    temp_df["Window Start Date"].append(generic_cus_tar_start)
    temp_df["Window End Date"].append(generic_cus_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# Behavioural PD V2.1 PSI
psi_df = data_pull(
    f"""
    select accountId, referenceDate, brand, bad_rate, modelScore
    from neo_data_science_production.credit_risk_behavior_pd_v2_1 as a
    where referenceDate = '{generic_cus_start}'
    or referenceDate = '{generic_cus_tar_end}'
  """
)

psi_this = psi_df.toPandas()
psi_this["referenceDate"] = pd.to_datetime(psi_this["referenceDate"])

temp_df = {
    "Model": ["Behavioural PD V2.1"],
    "t0 Date": [generic_cus_start],
    "t1 Date": [generic_cus_tar_end],
    "PSI": [
        psi(
            psi_this,
            "bad_rate",
            "referenceDate",
            pd.to_datetime(generic_cus_start),
            pd.to_datetime(generic_cus_tar_end),
        )
    ],
    "PSI Bin Count": [5],
    "Executed": [datetime.today()],
}

psi_res = pd.concat([psi_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Behavioural BKO V1.1

# COMMAND ----------

# Behavioural BKO V1.1 AUC
pop_df = data_pull(
    f"""
    select accountId, brand, score, probability
    from neo_views_credit_risk.beh_bko_score_test as a
    where monthend = '{generic_cus_start}'
  """
)

target_df = data_pull(
    f"""
    select distinct accountId, 1 as co
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{generic_cus_tar_start}' and '{generic_cus_tar_end}'
    and chargedOffReason in   ("CONSUMER_PROPOSALS",
    "BANKRUPTCY",
    "CREDIT_COUNSELLING_SOLUTIONS",
    "SETTLEMENTS")
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")
monitor_df = monitor_df.withColumn("is_co", when(col("co") == 1, 1).otherwise(0))
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["Behavioural BKO V1.1"],
    "Pop Start Date": [generic_cus_start],
    "Pop End Date": [None],
    "Window Start Date": [generic_cus_tar_start],
    "Window End Date": [generic_cus_tar_end],
    "GINI": [2*(auc_calc(score_this, "probability", "is_co")-0.5)],
    "KS": [ks_calc(score_this, "probability", "is_co")],
    "AUC": [auc_calc(score_this, "probability", "is_co")],
    "Executed": [datetime.today()],
}

score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]

seg_calc = by_segment("brand", score_this, "probability", "is_co")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"Behavioural BKO V1.1 - {n}")
    temp_df["Pop Start Date"].append(generic_cus_start)
    temp_df["Pop End Date"].append(None)
    temp_df["Window Start Date"].append(generic_cus_tar_start)
    temp_df["Window End Date"].append(generic_cus_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# Behavioural BKO V1.1 PSI
psi_df = data_pull(
    f"""
    select accountId, monthend, brand, score, probability
    from neo_views_credit_risk.beh_bko_score_test as a
    where monthend = '{generic_cus_start}'
    or monthend = '{generic_cus_tar_end}'
  """
)

psi_this = psi_df.toPandas()
psi_this["monthend"] = pd.to_datetime(psi_this["monthend"])

temp_df = {
    "Model": ["Behavioural BKO V1.1"],
    "t0 Date": [generic_cus_start],
    "t1 Date": [generic_cus_tar_end],
    "PSI": [
        psi(
            psi_this,
            "probability",
            "monthend",
            pd.to_datetime(generic_cus_start),
            pd.to_datetime(generic_cus_tar_end),
        )
    ],
    "PSI Bin Count": [5],
    "Executed": [datetime.today()],
}

psi_res = pd.concat([psi_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Oscar

# COMMAND ----------

# Oscar AUC
pop_df = data_pull(
    f"""
    select creditAccountId as accountId, brand, score
    from neo_data_science_production.credit_risk_collection_probability_to_repay_daily
    where referenceDate = '{oscar_start}'
  """
)

target_df = data_pull(
    f"""
    select distinct accountId, 1 as is_cured
    from neo_trusted_analytics.earl_account
    where referenceDate between '{oscar_tar_start}' and '{oscar_tar_end}'
    and (daysPastDueBucket = "B0. Current")
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")

monitor_df = monitor_df.withColumn(
    "is_cured", when(col("is_cured") == 1, 1).otherwise(0)
)
score_this = monitor_df.toPandas()
temp_df = {
    "Model": ["Oscar - Probability to Cure"],
    "Pop Start Date": [oscar_start],
    "Pop End Date": [None],
    "Window Start Date": [oscar_tar_start],
    "Window End Date": [oscar_tar_end],
    "GINI": [2*(auc_calc(score_this, "score", "is_cured")-0.5)],
    "KS": [ks_calc(score_this, "score", "is_cured")],
    "AUC": [auc_calc(score_this, "score", "is_cured")],
    "Executed": [datetime.today()],
}

score_this = score_this[score_this["brand"].isin(["NEO", "HBC", "SIENNA", "CATHAY"])]

seg_calc = by_segment("brand", score_this, "score", "is_cured")

for n, gini, ks, auc in seg_calc:
    temp_df["Model"].append(f"Oscar - Probability to Cure - {n}")
    temp_df["Pop Start Date"].append(oscar_start)
    temp_df["Pop End Date"].append(None)
    temp_df["Window Start Date"].append(oscar_tar_start)
    temp_df["Window End Date"].append(oscar_tar_end)
    temp_df["GINI"].append(gini),
    temp_df["KS"].append(ks),
    temp_df["AUC"].append(auc)
    temp_df["Executed"].append(datetime.today())

monitoring_res = pd.concat([monitoring_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# Oscar PSI
psi_df = data_pull(
    f"""
    select referenceDate, creditAccountId as accountId, brand, score
    from neo_data_science_production.credit_risk_collection_probability_to_repay_daily
    where referenceDate = '{oscar_start}'
    or referenceDate = '{generic_cus_tar_end}'
  """
)

psi_this = psi_df.toPandas()
psi_this["referenceDate"] = pd.to_datetime(psi_this["referenceDate"])

temp_df = {
    "Model": ["Oscar - Probability to Cure"],
    "t0 Date": [oscar_start],
    "t1 Date": [generic_cus_tar_end],
    "PSI": [
        psi(
            psi_this,
            "score",
            "referenceDate",
            pd.to_datetime(oscar_start),
            pd.to_datetime(generic_cus_tar_end),
        )
    ],
    "PSI Bin Count": [5],
    "Executed": [datetime.today()],
}

psi_res = pd.concat([psi_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## CreditVision CSI

# COMMAND ----------

# TU CSI
csi_df = data_pull(
    f"""
    SELECT *, concat(cast(year(createdat) as string), cast(month(createdat) as string)) as filter_dt
    FROM neo_raw_production.transunion_creditreport_creditvision
    WHERE month(createdat) = {generic_psi_mmyy_t0[1]} and year(createdat) in ({generic_psi_mmyy_t0[0]}, {generic_psi_mmyy_t1[0]})
  """
)

# csi_this = csi_df.toPandas()

temp_df = {
    "Model": [],
    "t0 Date": [],
    "t1 Date": [],
    "CSI": [],
    "CSI Bin Count": [],
    "Executed": [],
}

col_list = [
    "GO07",
    "CVSC100",
    "AM07",
    "GO21",
    "AM36",
    "AM44",
    "BC94",
    "GO11",
    "BC91",
    "RE02",
    "RE91",
    "AT34",
    "BC142",
    "BC145",
    "GO17",
    "GO152",
    "GO11",
    "AT01",
    "GO141",
    "BC62",
    "RR91",
    "GO14",
    "RE29",
    "AM33",
    "AM167",
    "AT60",
    "BC60",
    "RE03",
    "GO148",
    "AM42",
    "AM84",
    "AM04",
    "GO152",
    "AT07",
    "RE28",
]

for c in col_list:
    try:
        temp = csi_df.select(c, "filter_dt").toPandas()
        temp[c] = temp[c].astype("float")
        temp_df["Model"].append(f"CreditVision - {c}")
        temp_df["t0 Date"].append(
            f"y{str(generic_psi_mmyy_t0[0])}m{str(generic_psi_mmyy_t0[1])}"
        )
        temp_df["t1 Date"].append(
            f"y{str(generic_psi_mmyy_t1[0])}m{str(generic_psi_mmyy_t1[1])}"
        )
        temp_df["CSI"].append(
            psi(
                temp,
                c,
                "filter_dt",
                f"{str(generic_psi_mmyy_t0[0])}{str(generic_psi_mmyy_t0[1])}",
                f"{str(generic_psi_mmyy_t1[0])}{str(generic_psi_mmyy_t1[1])}",
            )
        )
        temp_df["CSI Bin Count"].append(5)
        temp_df["Executed"].append(datetime.today())
    except:
        print(f"error - {c}")


csi_res = pd.concat([csi_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## Earl CSI

# COMMAND ----------

# EARL CSI
csi_df = data_pull(
    f"""
    select referenceDate, postedCreditAccountBalanceDollars from neo_trusted_analytics.earl_account
    where referenceDate = '{generic_cus_start}'
    or referenceDate = '{generic_cus_tar_end}'
  """
)

temp_df = {
    "Model": [],
    "t0 Date": [],
    "t1 Date": [],
    "CSI": [],
    "CSI Bin Count": [],
    "Executed": [],
}

col_list = [
    "postedCreditAccountBalanceDollars",
]

for c in col_list:
    temp = csi_df.select(c, "referenceDate").toPandas()
    temp[c] = temp[c].astype("float")
    temp["referenceDate"] = pd.to_datetime(temp["referenceDate"])
    temp_df["Model"].append(f"EARL - {c}")
    temp_df["t0 Date"].append(generic_cus_start)
    temp_df["t1 Date"].append(generic_cus_tar_end)
    temp_df["CSI"].append(
        psi(
            temp,
            c,
            "referenceDate",
            pd.to_datetime(generic_cus_start),
            pd.to_datetime(generic_cus_tar_end),
        )
    )
    temp_df["CSI Bin Count"].append(5)
    temp_df["Executed"].append(datetime.today())

csi_res = pd.concat([csi_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md ## BEH/BKO grids

# COMMAND ----------

# Behavioural BKO V1.1 Grid
pop_df = data_pull(
    f"""
    select accountId, brand, score, probability, monthend
    from neo_views_credit_risk.beh_bko_score_test as a
    where monthend = '{generic_cus_start}'
  """
)

target_df = data_pull(
    f"""
    select accountId, 1 as co, min(referenceDate) as acc_co_dt
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{generic_cus_tar_start}' and '{generic_cus_tar_end}'
    and chargedOffReason in   ("CONSUMER_PROPOSALS",
    "BANKRUPTCY",
    "CREDIT_COUNSELLING_SOLUTIONS",
    "SETTLEMENTS")
    group by accountId
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")

monitor_df = monitor_df.withColumn(
    "datediff", datediff(col("acc_co_dt"), col("monthend"))
)

grid_this = monitor_df.toPandas()

temp_df = grid(grid_this, "Behavioural BKO V1.1", "probability", "co", "datediff", f"{generic_cus_start}")

grid_res = pd.concat([grid_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# Behavioural PD V2.1 Grid
pop_df = data_pull(
    f"""
    select accountId, brand, bad_rate, modelScore
    from neo_data_science_production.credit_risk_behavior_pd_v2_1 as a
    where referenceDate = '{generic_cus_start}'
  """
)

target_df = data_pull(
    f"""
    select accountId, 1 as is_def, min(referenceDate) as def_dt
    from neo_trusted_analytics.earl_account
    where referenceDate between '{generic_cus_tar_start}' and '{generic_cus_tar_end}'
    and (chargeOffCategory = "CREDIT" or is90daysDPD)
    group by accountId
  """
)

monitor_df = pop_df.join(target_df, on="accountId", how="left")

monitor_df = monitor_df.withColumn(
    "datediff", datediff(col("def_dt"), col("referenceDate"))
)

grid_this = monitor_df.toPandas()

temp_df = grid(grid_this, "Behavioural PD V2.1", "bad_rate", "is_def", "datediff", f"{generic_cus_start}")

grid_res = pd.concat([grid_res, pd.DataFrame(temp_df)], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Data

# COMMAND ----------

psi_res

# COMMAND ----------

csi_res

# COMMAND ----------

monitoring_res

# COMMAND ----------

grid_res

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Data

# COMMAND ----------

temp = spark.createDataFrame(monitoring_res)
temp.write.format("delta").mode("overwrite").saveAsTable(
    "neo_views_credit_risk.wk_mm_monitoring_res"
)
temp = spark.createDataFrame(csi_res)
temp.write.format("delta").mode("overwrite").saveAsTable(
    "neo_views_credit_risk.wk_mm_csi_res"
)
temp = spark.createDataFrame(psi_res)
temp.write.format("delta").mode("overwrite").saveAsTable(
    "neo_views_credit_risk.wk_mm_psi_res"
)
temp = spark.createDataFrame(grid_res)
temp.write.format("delta").mode("overwrite").saveAsTable(
    "neo_views_credit_risk.wk_mm_grid_res"
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

monitoring_res

# COMMAND ----------


