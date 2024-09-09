# Databricks notebook source
# MAGIC %md
# MAGIC #Score model and save results for monitoring purpose

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

checkyear = 2023
checkmonth = 8

# COMMAND ----------

# Extract raw data
raw_data = spark.sql(
  f"""
  select
  ea._id,
  ea.userId,
  ea.userReportsMetadataSnapshotId,
  ea.brand,
  cast(
    ms.transunionSoftCreditCheckResult.creditScore as int
  ) as creditScore,
  year(ea.createdAt) as yr,
  month(ea.createdAt) as mth,
  sc.*,
  case when usu.housingStatus = "RENT" then 1 else 0 end as houseStat_RENT
from
  neo_raw_production.credit_onboarding_service_credit_applications as ea
  inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms on ea.userReportsMetadataSnapshotId = ms._id
  inner join (select
  *
from
  (
    select
      sc_id,
      explode(params)
    from
      (
        SELECT
          sc_id,
          MAP_FROM_ENTRIES(
            COLLECT_LIST(
              STRUCT(
                accountNetCharacteristics.id,
                accountNetCharacteristics.value
              )
            )
          ) params
        FROM
          (
            (
              select
                _id as sc_id,
                details.accountNetCharacteristics
              from
                neo_raw_production.identity_service_transunion_soft_credit_check_reports
            ) as sc
          ) LATERAL VIEW INLINE(accountNetCharacteristics) accountNetCharacteristics
        GROUP BY
          sc_id
      )
  ) PIVOT (
    SUM(CAST(value AS INT)) AS crcValue FOR key IN (
      'AT01',
      'GO14',
      'GO15',
      'GO151',
      'GO141',
      'BC147',
      'BC94',
      'BC142',
      'AM07',
      'AM04',
      'BR04',
      'BC04',
      'AM167',
      'BR60',
      'AT60',
      'RE28',
      'BC60',
      'GO148',
      'BC62',
      'AM60',
      'AM41',
      'RE61',
      'AM33',
      'BC148',
      'RE07',
      'GO21',
      'RE01',
      'GO06',
      'AT02',
      'BC02',
      'AM91',
      'RE29',
      'BC145',
      'RE03',
      'RE06',
      'AM29',
      'RE336',
      'AM02'
    )
  )) as sc on ms.transunionSoftCreditCheckResult.reportId = sc.sc_id
  inner join neo_raw_production.user_service_users as usu on ea.userId = usu._id
where
  decision = "APPROVED"
  and year(ea.createdAt) = {checkyear}
  and month(ea.createdAt) = {checkmonth}
  """
)

# COMMAND ----------

#prepare initial data for psi/csi
#save to wk_appl_v2_for_stability

# COMMAND ----------

display(raw_data)

# COMMAND ----------

def stability_calc(base_dat, new_dat, var):
  #sum of (init % - new % * ln ( init % / new % ) )
  pass

# COMMAND ----------



# COMMAND ----------

display(raw_data)

# COMMAND ----------

complete_feature_set = set()
for mod_name in model_list:
  bst.load_model(model_save_path + mod_name)
  complete_feature_set.update(bst.feature_names_in_)
complete_feature_set = complete_feature_set.difference(['creditScore', 'houseStat_RENT'])
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
          when housingStatus = 'RENT' then 1
          else 0
        end as houseStat_RENT,
        case
          when (
            AT01 <= 0
            or GO14 <= 24
          ) then 1
          else 0
        end as thin,
        case
          when (creditScore < 640) then 1
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

  # indat_sp = spark.createDataFrame(indat)
  # indat_sp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(upload_name)
  # return indat[['model_scr', 'decision']].groupby(['decision']).mean()

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
