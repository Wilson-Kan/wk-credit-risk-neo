# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.brand,
# MAGIC   a.applicationId,
# MAGIC   a.ever90DPD_12M,
# MAGIC   a.accountOpenDate,
# MAGIC   last_day(a.accountOpenDate) as month_end,
# MAGIC   b.segment,
# MAGIC   b.bkoScoreV1,
# MAGIC   b.thickScoreV1,
# MAGIC   b.thinScoreV1,
# MAGIC   b.subprimeScoreV1
# MAGIC FROM
# MAGIC   `hive_metastore`.`neo_views_credit_risk`.`cc_all_v1000_dbt_application_performance_earl` as a
# MAGIC   INNER JOIN neo_views_credit_risk.cc_all_v1000_dbt_application_scores as b on a.applicationId = b.applicationId

# COMMAND ----------

df = _sqldf.toPandas()

# COMMAND ----------

df["segment_regroup"] = df["segment"].apply(
    lambda x: "2x. subprime" if x in ["2. thick_subprime", "4. thin_subprime"] else x
)


def set_score(row):
    if row["segment"] in ["2. thick_subprime", "4. thin_subprime"]:
        return -1*row["subprimeScoreV1"]
    elif row["segment"] in ["1. thick_prime"]:
        return -1*row["thickScoreV1"]
    elif row["segment"] in ["3. thin_prime"]:
        return -1*row["thinScoreV1"]
    else:
        return -1*row["thickScoreV1"]


df["score_regroup"] = df.apply(set_score, axis=1)

# COMMAND ----------

brands = df["brand"].unique()
segments = df["segment_regroup"].unique()
month_ends = df["month_end"].unique()
result = {"brand": [], "segment": [], "month_end": [], "AUC": []}

# COMMAND ----------

from sklearn.metrics import roc_auc_score

for b in brands:
    for s in segments:
        for me in month_ends:
            temp_df = df[
                (df["brand"] == b)
                & (df["segment_regroup"] == s)
                & (df["month_end"] == me)
            ]
            true_y = temp_df["ever90DPD_12M"].values
            pred_y = temp_df["score_regroup"].values
            result["brand"].append(b)
            result["segment"].append(s)
            result["month_end"].append(me)
            try:
                result["AUC"].append(2 * roc_auc_score(true_y, pred_y) - 1)
            except:
                result["AUC"].append(0)

# COMMAND ----------

import pandas as pd

res_df = pd.DataFrame(result)
res_df

# COMMAND ----------

res_df.to_csv(
    "/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Misc/Gini_by_brand.csv",
    index=False
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
