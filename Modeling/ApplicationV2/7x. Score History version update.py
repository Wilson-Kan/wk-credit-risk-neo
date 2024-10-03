# Databricks notebook source
# MAGIC %md
# MAGIC #Score model against old population

# COMMAND ----------

# MAGIC %pip install xgboost==2.0.3

# COMMAND ----------

from pyspark.sql import SparkSession
from xgboost import XGBClassifier
import math
import xgboost as xgb
import pandas as pd

spark = SparkSession.builder.getOrCreate()
bst = XGBClassifier()
pd.DataFrame.iteritems = pd.DataFrame.items

model_save_path = (
    "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/"
)
model_list = [
    "neo_thick.json",
    "neo_thin.json",
    "neo_subprime.json",
    "tims_thick_no_sp.json",
    "tims_thin_no_sp.json",
]

# COMMAND ----------

complete_feature_set = set()
for mod_name in model_list:
  bst.load_model(model_save_path + mod_name)
  complete_feature_set.update(bst.feature_names_in_)
complete_feature_set = complete_feature_set.difference(['creditScore'])
s = ""
for i in complete_feature_set:
  s += i + ","

# COMMAND ----------

score_me = spark.sql(
    f"""
      select
        brand,
        decision,
        createdAt,
        _id as applicationId,
        userId,
        creditFacility,
        cast(
          transunionSoftCreditCheckResult.creditScore as int
        ) as creditScore,
        case
          when (
            AT01 <= 0
            or GO14 <= 24
          ) then 1
          else 0
        end as thin,
        case
          when (cast(
          transunionSoftCreditCheckResult.creditScore as int
        )  < 640) then 1
          else 0
        end as subprime,
        {s[:-1]}
      from
        neo_views_credit_risk.wk_feature_and_target_no_hc
      where
        brand in ("NEO", "SIENNA")
        and type = 'STANDARD'
    """
)
score_me_df = score_me.toPandas()

# COMMAND ----------

def scaling_score(raw_score, lower_bound = 300, upper_bound = 850, round_down = True, inverse_scoring = True):
  if inverse_scoring:
    raw_score = raw_score * -1
  min_score = min(raw_score)
  max_score = max(raw_score)
  scale_0_1 = (raw_score - min_score) / (max_score - min_score)
  scale_to_tu_range = (scale_0_1 * (upper_bound - lower_bound)) + lower_bound
  print(f"scale_0_1 = (X - {min_score} / ({max_score} - {min_score}) \n scale_to_tu_range = (scale_0_1 * (850 - 300)) + 300))")
  if round_down:
    return [math.floor(i) for i in scale_to_tu_range]
  return scale_to_tu_range
def score_upload(model, indat, upload_name):

  indat.loc[:,'raw_pred'] = bst.predict_proba(indat[bst.feature_names_in_])[:,1]
  indat.loc[:,'model_scr'] = scaling_score(indat['raw_pred'])

  indat_sp = spark.createDataFrame(indat)
  indat_sp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(upload_name)
  return indat[['model_scr', 'decision']].groupby(['decision']).mean()

# COMMAND ----------

bst.load_model(model_save_path + model_list[0])
neo_thick_df = score_me_df[(score_me_df["thin"] == 0) & (score_me_df["subprime"] == 0) & (score_me_df["brand"] == 'NEO')]
score_upload(bst, neo_thick_df, "hive_metastore.neo_views_credit_risk.wk_neo_thick_app_v2")

# COMMAND ----------

bst.load_model(model_save_path + model_list[1])
neo_thin_df = score_me_df[(score_me_df["thin"] == 1) & (score_me_df["subprime"] == 0) & (score_me_df["brand"] == 'NEO')]
score_upload(bst, neo_thin_df, "hive_metastore.neo_views_credit_risk.wk_neo_thin_app_v2")

# COMMAND ----------

bst.load_model(model_save_path + model_list[2])
neo_sp_df = score_me_df[(score_me_df["subprime"] == 1) & (score_me_df["brand"] == 'NEO')]
score_upload(bst, neo_sp_df, "hive_metastore.neo_views_credit_risk.wk_neo_subprime_app_v2")

# COMMAND ----------

bst.load_model(model_save_path + model_list[3])
tims_thick_df = score_me_df[(score_me_df["thin"] == 0) & (score_me_df["brand"] == 'SIENNA')]
score_upload(bst, tims_thick_df, "hive_metastore.neo_views_credit_risk.wk_tims_thick_app_v2")

# COMMAND ----------

bst.load_model(model_save_path + model_list[4])
tims_thin_df = score_me_df[(score_me_df["thin"] == 1) & (score_me_df["brand"] == 'SIENNA')]
score_upload(bst, tims_thin_df, "hive_metastore.neo_views_credit_risk.wk_tims_thin_app_v2")
