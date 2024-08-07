# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.applicationId,
# MAGIC   a.brand,
# MAGIC   a.isdefault_1y,
# MAGIC   case
# MAGIC     when creditScore < 640 then 1
# MAGIC     else 0
# MAGIC   end as subprime,
# MAGIC   case
# MAGIC     when (
# MAGIC       (
# MAGIC         AT01 <= 0
# MAGIC         or GO14 <= 24
# MAGIC       )
# MAGIC       and subprime = 0
# MAGIC     ) then 1
# MAGIC     else 0
# MAGIC   end as thin,
# MAGIC   b.segment,
# MAGIC   b.bkoScoreV1,
# MAGIC   b.thickScoreV1 * -1 as thickscr,
# MAGIC   b.thinScoreV1 * -1 as thinscr,
# MAGIC   b.subprimeScoreV1  * -1 as spscr
# MAGIC FROM
# MAGIC   neo_views_credit_risk.wk_feature_and_target_no_hc as a
# MAGIC   INNER JOIN neo_views_credit_risk.cc_all_v1000_dbt_application_scores as b on a.applicationId = b.applicationId
# MAGIC where
# MAGIC   a.brand = 'NEO'
# MAGIC   and a.decision = 'APPROVED'
# MAGIC   and a.type = 'STANDARD'

# COMMAND ----------

from sklearn.metrics import roc_auc_score

existing_score = _sqldf.toPandas()

thick = existing_score[(existing_score["thin"] + existing_score["subprime"] == 0) & (existing_score["thickscr"] < 0)]
thin = existing_score[(existing_score["thin"] == 1) & (existing_score["thinscr"] < 0)]
sp = existing_score[(existing_score["subprime"] == 1) & (existing_score["spscr"] < 0)]

print("thick", 2 * roc_auc_score(thick["isdefault_1y"], thick["thickscr"]) - 1)
print("thin", 2 * roc_auc_score(thin["isdefault_1y"], thin["thinscr"]) - 1)
print("sp", 2 * roc_auc_score(sp["isdefault_1y"], sp["spscr"]) - 1)

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
