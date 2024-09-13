# Databricks notebook source
# MAGIC %md
# MAGIC #Score model and save results for monitoring purpose

# COMMAND ----------

import datetime
import dateutil.relativedelta

t_date = datetime.datetime.today()
st_date_1 = t_date - dateutil.relativedelta.relativedelta(years=1, months=6)
st_date_2 = t_date - dateutil.relativedelta.relativedelta(years=1)
st_date_1 = st_date_1.replace(day=1)
st_date_2 = st_date_2.replace(day=1)
t_date = t_date.replace(day=1)

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
model_id_list = [
    "neo_thick",
    "neo_thin",
    "neo_subprime",
    "tims_thick",
    "tims_thin",
]

# COMMAND ----------

# # Extract raw data
# raw_data = spark.sql(
#   f"""
#   select
#   ea._id,
#   ms.applicationId,
#   ea.userId,
#   ea.userReportsMetadataSnapshotId,
#   ea.brand,
#   ea.type,
#   cast(
#     ms.transunionSoftCreditCheckResult.creditScore as int
#   ) as creditScore,
#   year(ea.createdAt) as yr,
#   month(ea.createdAt) as mth,
#   sc.*,
#   case when usu.housingStatus = "RENT" then 1 else 0 end as houseStat_RENT
# from
#   neo_raw_production.credit_onboarding_service_credit_applications as ea
#   inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms on ea.userReportsMetadataSnapshotId = ms._id
#   inner join (select
#   *
# from
#   (
#     select
#       sc_id,
#       explode(params)
#     from
#       (
#         SELECT
#           sc_id,
#           MAP_FROM_ENTRIES(
#             COLLECT_LIST(
#               STRUCT(
#                 accountNetCharacteristics.id,
#                 accountNetCharacteristics.value
#               )
#             )
#           ) params
#         FROM
#           (
#             (
#               select
#                 _id as sc_id,
#                 details.accountNetCharacteristics
#               from
#                 neo_raw_production.identity_service_transunion_soft_credit_check_reports
#             ) as sc
#           ) LATERAL VIEW INLINE(accountNetCharacteristics) accountNetCharacteristics
#         GROUP BY
#           sc_id
#       )
#   ) PIVOT (
#     SUM(CAST(value AS INT)) AS crcValue FOR key IN (
#       'AT01',
#       'GO14',
#       'GO15',
#       'GO151',
#       'GO141',
#       'BC147',
#       'BC94',
#       'BC142',
#       'AM07',
#       'AM04',
#       'BR04',
#       'BC04',
#       'AM167',
#       'BR60',
#       'AT60',
#       'RE28',
#       'BC60',
#       'GO148',
#       'BC62',
#       'AM60',
#       'AM41',
#       'RE61',
#       'AM33',
#       'BC148',
#       'RE07',
#       'GO21',
#       'RE01',
#       'GO06',
#       'AT02',
#       'BC02',
#       'AM91',
#       'RE29',
#       'BC145',
#       'RE03',
#       'RE06',
#       'AM29',
#       'RE336',
#       'AM02'
#     )
#   )) as sc on ms.transunionSoftCreditCheckResult.reportId = sc.sc_id
#   inner join neo_raw_production.user_service_users as usu on ea.userId = usu._id
# where
#   decision = "APPROVED"
#   and ea.createdAt between '{st_date_1}' and '{st_date_2}'
#   and ea.type = "STANDARD"
#   """
# )

# default_list = spark.sql(
#   f"""
#     select distinct applicationId, userId, 1 as def
#       FROM
#         neo_trusted_analytics.earl_account
#       where
#       (
#         daysPastDue >= 90
#         or chargedOffReason not in ("N/A")
#       )
#       and referenceDate between '{st_date_1}' and '{t_date}'
#       and brand in ('NEO', 'SIENNA')
#   """
# )

# score_me = raw_data.alias("a").join(default_list.alias("b"), ((raw_data.applicationId == default_list.applicationId) & (raw_data.userId == default_list.userId)), how="left").select("a.*", "b.def")
# score_me = score_me.na.fill(value=0,subset=["def"])
# score_me = score_me.toPandas()

# COMMAND ----------

# display(default_list)

# COMMAND ----------

# Export/Import pickle file

import pickle

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_v2_monitoring_temp.pkl",
#     "wb",
# ) as f:  # open a text file
#     pickle.dump(score_me, f)

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_v2_monitoring_temp.pkl",
#     "rb",
# ) as f:  # Correctly opening the file in binary read mode
#     score_me = pickle.load(f)

# COMMAND ----------

# Normalize softcheck

tu_sc = spark.sql(
    """select * from (select sc_id, explode(params) from (SELECT 
          sc_id
          ,MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(accountNetCharacteristics.id, accountNetCharacteristics.value))) params
        FROM ((select _id as sc_id
          ,details.accountNetCharacteristics from neo_raw_production.identity_service_transunion_soft_credit_check_reports
          union select _id as sc_id
          ,details.accountNetCharacteristics from neo_raw_production.application_service_transunion_soft_credit_reports) as sc) LATERAL VIEW INLINE(accountNetCharacteristics) accountNetCharacteristics
        GROUP BY sc_id))
    PIVOT (
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
    )
  """
)

df_w_sc = spark.sql(
    """
    select
      ea.brandName as brand,
      ea.applicationCompletedAt_mt as app_date,
      ms._id as ms_id,
      ms.applicationId,
      ms.transunionSoftCreditCheckResult,
          cast(
            ms.transunionSoftCreditCheckResult.creditScore as int
          ) as creditScore,
          case
            when cast(
            ms.transunionSoftCreditCheckResult.creditScore as int
          ) < 640 then 1
            else 0
          end as subprime,
      usu._id as usu_id,
      case when usu.housingStatus = 'RENT' then 1 else 0 end as houseStat_RENT
      from
  neo_trusted_analytics.earl_application as ea
  inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms on ea.applicationId = ms.applicationId
        inner join neo_raw_production.user_service_users as usu
        on ea.customerId = usu._id
        where ea.cardType = 'STANDARD' and ea.applicationDecision = 'APPROVED'
  """
)

# Create default data

def_list = spark.sql(
    """
    select * from
    (
      SELECT
      earl_acc.applicationId,
      earl_acc.referenceDate,
      earl_acc.productTypeName,
      earl_acc.chargedOffReason,
      earl_acc.monthOnBook,
      earl_acc.creditFacility,
      1 as isdefault,
      row_number() OVER(
        PARTITION BY earl_acc.accountId
        ORDER BY
          earl_acc.referenceDate
      ) AS n
      FROM
        neo_trusted_analytics.earl_account as earl_acc
      where
      (
        daysPastDue >= 90
        or chargedOffReason not in ("N/A")
      )
    order by
      earl_acc.applicationId,
      earl_acc.referenceDate
    )
  where n = 1
"""
)

temp_a = df_w_sc.join(
    tu_sc, df_w_sc.transunionSoftCreditCheckResult.reportId == tu_sc.sc_id, "left"
)
res = temp_a.join(def_list, temp_a.applicationId == def_list.applicationId, "left")

res = res.filter(res.app_date.between(st_date_1, st_date_2))
res = res.fillna(0, subset=["isdefault"])
res = res.select(
    "brand",
    "creditScore",
    "subprime",
    "houseStat_RENT",
    "isdefault",
    "ms.applicationId",
    "AT01",
    "GO14",
    "GO15",
    "GO151",
    "GO141",
    "BC147",
    "BC94",
    "BC142",
    "AM07",
    "AM04",
    "BR04",
    "BC04",
    "AM167",
    "BR60",
    "AT60",
    "RE28",
    "BC60",
    "GO148",
    "BC62",
    "AM60",
    "AM41",
    "RE61",
    "AM33",
    "BC148",
    "RE07",
    "GO21",
    "RE01",
    "GO06",
    "AT02",
    "BC02",
    "AM91",
    "RE29",
    "BC145",
    "RE03",
    "RE06",
    "AM29",
    "RE336",
    "AM02",
)

# COMMAND ----------

# # Normalize softcheck

# tu_sc = spark.sql(
#     """select * from (select sc_id, explode(params) from (SELECT
#           sc_id
#           ,MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(accountNetCharacteristics.id, accountNetCharacteristics.value))) params
#         FROM ((select _id as sc_id
#           ,details.accountNetCharacteristics from neo_raw_production.identity_service_transunion_soft_credit_check_reports
#           union select _id as sc_id
#           ,details.accountNetCharacteristics from neo_raw_production.application_service_transunion_soft_credit_reports) as sc) LATERAL VIEW INLINE(accountNetCharacteristics) accountNetCharacteristics
#         GROUP BY sc_id))
#     PIVOT (
#           SUM(CAST(value AS INT)) AS crcValue FOR key IN (
#       'AT01',
#       'GO14',
#       'GO15',
#       'GO151',
#       'GO141',
#       'BC147',
#       'BC94',
#       'BC142',
#       'AM07',
#       'AM04',
#       'BR04',
#       'BC04',
#       'AM167',
#       'BR60',
#       'AT60',
#       'RE28',
#       'BC60',
#       'GO148',
#       'BC62',
#       'AM60',
#       'AM41',
#       'RE61',
#       'AM33',
#       'BC148',
#       'RE07',
#       'GO21',
#       'RE01',
#       'GO06',
#       'AT02',
#       'BC02',
#       'AM91',
#       'RE29',
#       'BC145',
#       'RE03',
#       'RE06',
#       'AM29',
#       'RE336',
#       'AM02'
#       )
#     )
#   """
# )

# df_w_sc = spark.sql(
#     """
#     select
#       ea.*,
#       ms._id as ms_id,
#       ms.applicationId,
#       ms.transunionSoftCreditCheckResult,
#           cast(
#             ms.transunionSoftCreditCheckResult.creditScore as int
#           ) as creditScore,
#           case
#             when cast(
#             ms.transunionSoftCreditCheckResult.creditScore as int
#           ) < 640 then 1
#             else 0
#           end as subprime,
#       usu._id as usu_id,
#       case when usu.housingStatus = 'RENT' then 1 else 0 end as houseStat_RENT
#       from
#         neo_raw_production.credit_onboarding_service_credit_applications as ea
#         inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms
#         on ea.userReportsMetadataSnapshotId = ms._id
#         inner join neo_raw_production.user_service_users as usu
#         on ea.userId = usu._id
#   """
# )

# # Create default data

# def_list = spark.sql(
#     """
#     select * from
#     (
#       SELECT
#       applicationId,
#       referenceDate,
#       productTypeName,
#       chargedOffReason,
#       monthOnBook,
#       creditFacility,
#       1 as isdefault,
#       row_number() OVER(
#         PARTITION BY accountId
#         ORDER BY
#           referenceDate
#       ) AS n
#       FROM
#         neo_trusted_analytics.earl_account
#       where
#       (
#         daysPastDue >= 90
#         or chargedOffReason not in ("N/A")
#       )
#     order by
#       applicationId,
#       referenceDate
#     )
#   where n = 1
# """
# )

# temp_a = df_w_sc.join(
#     tu_sc, df_w_sc.transunionSoftCreditCheckResult.reportId == tu_sc.sc_id, "left"
# )
# res = temp_a.join(def_list, temp_a.applicationId == def_list.applicationId, "left")

# res = res.filter(res.createdAt.between(st_date_1, st_date_2))
# res = res.fillna(0, subset=["isdefault"])
# res = res.select(
#     "createdAt",
#     "brand",
#     "creditScore",
#     "subprime",
#     "houseStat_RENT",
#     "isdefault",
#     "ms.applicationId",
#     "AT01",
#     "GO14",
#     "GO15",
#     "GO151",
#     "GO141",
#     "BC147",
#     "BC94",
#     "BC142",
#     "AM07",
#     "AM04",
#     "BR04",
#     "BC04",
#     "AM167",
#     "BR60",
#     "AT60",
#     "RE28",
#     "BC60",
#     "GO148",
#     "BC62",
#     "AM60",
#     "AM41",
#     "RE61",
#     "AM33",
#     "BC148",
#     "RE07",
#     "GO21",
#     "RE01",
#     "GO06",
#     "AT02",
#     "BC02",
#     "AM91",
#     "RE29",
#     "BC145",
#     "RE03",
#     "RE06",
#     "AM29",
#     "RE336",
#     "AM02",
# )

# COMMAND ----------

score_me = res.toPandas()

# COMMAND ----------

score_me["Subprime"] = (score_me["creditScore"] < 640) * 1
score_me["Thin"] = ((score_me["GO14"] <= 24) | (score_me["AT01"] <= 0)) * 1


def model_id(row):
    if row["brand"] == "NEO":
        if row["Subprime"] == 1:
            return "neo_subprime"
        elif row["Thin"] == 1:
            return "neo_thin"
        else:
            return "neo_thick"
    elif row["brand"] == "SIENNA":
        if row["Thin"] == 1:
            return "tims_thin"
        else:
            return "tims_thick"
    return "other"


score_me["model_id"] = score_me.apply(model_id, axis=1)

# COMMAND ----------

from sklearn.metrics import roc_auc_score

res = {"model_id": [], "date": [], "count": [], "def_count": [], "auc": []}

for i in range(len(model_list)):
    print(i)
    bst.load_model(model_save_path + model_list[i])
    test_dat = score_me[score_me["model_id"] == model_id_list[i]]
    test_dat.loc[:, "raw_pred"] = bst.predict_proba(
        test_dat.loc[:, bst.feature_names_in_]
    )[:, 1]
    auc = 1
    def_c = test_dat["isdefault"].sum()
    # def_c = test_dat["def"].sum()
    if def_c > 0:
        # auc = 2 * roc_auc_score(test_dat.loc[:, "def"], test_dat.loc[:, "raw_pred"]) - 1
        auc = (
            2 * roc_auc_score(test_dat.loc[:, "isdefault"], test_dat.loc[:, "raw_pred"])
            - 1
        )
    res["model_id"].append(model_id_list[i])
    res["date"].append(f"{st_date_1.date()} - {st_date_2.date()}")
    res["auc"].append(auc)
    res["def_count"].append(def_c)
    res["count"].append(test_dat.shape[0])

# COMMAND ----------

pd.DataFrame(res)

# COMMAND ----------

# # prepare initial data for psi/csi
# # save to wk_appl_v2_for_stability
# import numpy as np
# import random

# from sklearn.model_selection import train_test_split

# psi_save = {}

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_neo_v2_modeling_ready.pkl",
#     "rb",
# ) as f:  # Correctly opening the file in binary read mode
#     modeling_dummy_df = pickle.load(f)

# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] < np.datetime64("2023-06-30")
# ]
# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] > np.datetime64("2022-12-31")
# ]
# modeling_intime = modeling_intime[
#     modeling_intime["thin"] + modeling_intime["subprime"] == 0
# ]

# # dropping duplicated features
# X_train, X_test, y_train, y_test = train_test_split(
#     modeling_intime.drop(
#         ["month_end", "isdefault_1y", "originalCreditScore", "GO17"], axis=1
#     ),
#     modeling_intime["isdefault_1y"],
#     test_size=0.2,
#     random_state=213987,
# )

# bst.load_model(model_save_path + model_list[0])
# features = bst.feature_names_in_
# X_train = X_train[features]
# for i in features:
#     X_train["temp"], X_b = pd.cut(
#         X_train[i], bins=10, include_lowest=True, retbins=True
#     )
#     psi_group = (
#         X_train[["temp"]].groupby("temp", observed=True).size() / X_train.shape[0]
#     )
#     psi_save[("neo_thick", i)] = (psi_group, X_b)

# X_train["isdefault_1y"] = y_train
# dr = (
#     X_train[["isdefault_1y"]].groupby("isdefault_1y", observed=True).size()
#     / X_train.shape[0]
# )
# psi_save[("neo_thick", "isdefault_1y")] = (dr, [0, 1])

# COMMAND ----------

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_neo_v2_modeling_ready.pkl",
#     "rb",
# ) as f:  # Correctly opening the file in binary read mode
#     modeling_dummy_df = pickle.load(f)

# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] < np.datetime64("2023-06-30")
# ]
# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] > np.datetime64("2022-12-31")
# ]

# modeling_intime = modeling_intime[modeling_intime["thin"] == 1]

# X_train, X_test, y_train, y_test = train_test_split(
#     modeling_intime.drop(
#         ["month_end", "isdefault_1y", "originalCreditScore", "GO17"], axis=1
#     ),
#     modeling_intime["isdefault_1y"],
#     test_size=0.2,
#     random_state=459339,
# )

# bst.load_model(model_save_path + model_list[1])
# features = bst.feature_names_in_
# X_train = X_train[features]
# for i in features:
#     X_train["temp"], X_b = pd.cut(
#         X_train[i], bins=10, include_lowest=True, retbins=True
#     )
#     psi_group = (
#         X_train[["temp"]].groupby("temp", observed=True).size() / X_train.shape[0]
#     )
#     psi_save[("neo_thin", i)] = (psi_group, X_b)

# X_train["isdefault_1y"] = y_train
# dr = (
#     X_train[["isdefault_1y"]].groupby("isdefault_1y", observed=True).size()
#     / X_train.shape[0]
# )
# psi_save[("neo_thin", "isdefault_1y")] = (dr, [0, 1])

# COMMAND ----------

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_neo_v2_modeling_ready.pkl",
#     "rb",
# ) as f:  # Correctly opening the file in binary read mode
#     modeling_dummy_df = pickle.load(f)

# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] <= np.datetime64("2024-03-31")
# ]
# modeling_intime = modeling_intime[modeling_intime["subprime"] == 1]
# X_train, X_test, y_train, y_test = train_test_split(
#     modeling_intime.drop(
#         ["month_end", "isdefault_1y", "originalCreditScore", "GO17"], axis=1
#     ),
#     modeling_intime["isdefault_1y"],
#     test_size=0.2,
#     random_state=651677,
# )
# bst.load_model(model_save_path + model_list[2])
# features = bst.feature_names_in_
# X_train = X_train[features]
# for i in features:
#     X_train["temp"], X_b = pd.cut(
#         X_train[i], bins=10, include_lowest=True, retbins=True
#     )
#     psi_group = (
#         X_train[["temp"]].groupby("temp", observed=True).size() / X_train.shape[0]
#     )
#     psi_save[("neo_subprime", i)] = (psi_group, X_b)
# X_train["isdefault_1y"] = y_train
# dr = (
#     X_train[["isdefault_1y"]].groupby("isdefault_1y", observed=True).size()
#     / X_train.shape[0]
# )
# psi_save[("neo_subprime", "isdefault_1y")] = (dr, [0, 1])

# COMMAND ----------

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_tim_v2_modeling_ready_no_sp.pkl",
#     "rb",
# ) as f:  # Correctly opening the file in binary read mode
#     modeling_dummy_df = pickle.load(f)

# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] < np.datetime64("2023-06-30")
# ]
# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] > np.datetime64("2022-12-31")
# ]
# modeling_intime = modeling_intime[modeling_intime["thin"] == 0]
# X_train, X_test, y_train, y_test = train_test_split(
#     modeling_intime.drop(
#         ["month_end", "isdefault_1y", "originalCreditScore", "GO17"], axis=1
#     ),
#     modeling_intime["isdefault_1y"],
#     test_size=0.2,
#     random_state=213987,
# )

# bst.load_model(model_save_path + model_list[3])
# features = bst.feature_names_in_
# X_train = X_train[features]
# for i in features:
#     X_train["temp"], X_b = pd.cut(
#         X_train[i], bins=10, include_lowest=True, retbins=True
#     )
#     psi_group = (
#         X_train[["temp"]].groupby("temp", observed=True).size() / X_train.shape[0]
#     )
#     psi_save[("tims_thick", i)] = (psi_group, X_b)
# X_train["isdefault_1y"] = y_train
# dr = (
#     X_train[["isdefault_1y"]].groupby("isdefault_1y", observed=True).size()
#     / X_train.shape[0]
# )
# psi_save[("tims_thick", "isdefault_1y")] = (dr, [0, 1])

# COMMAND ----------

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_tim_v2_modeling_ready_no_sp.pkl",
#     "rb",
# ) as f:  # Correctly opening the file in binary read mode
#     modeling_dummy_df = pickle.load(f)

# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] < np.datetime64("2023-06-30")
# ]
# modeling_intime = modeling_dummy_df[
#     modeling_dummy_df["month_end"] > np.datetime64("2022-12-31")
# ]
# modeling_intime = modeling_intime[modeling_intime["thin"] == 1]
# X_train, X_test, y_train, y_test = train_test_split(
#     modeling_intime.drop(
#         ["month_end", "isdefault_1y", "originalCreditScore", "GO17"], axis=1
#     ),
#     modeling_intime["isdefault_1y"],
#     test_size=0.2,
#     random_state=459339,
# )

# bst.load_model(model_save_path + model_list[4])
# features = bst.feature_names_in_
# X_train = X_train[features]
# for i in features:
#     X_train["temp"], X_b = pd.cut(
#         X_train[i], bins=10, include_lowest=True, retbins=True
#     )
#     psi_group = (
#         X_train[["temp"]].groupby("temp", observed=True).size() / X_train.shape[0]
#     )
#     psi_save[("tims_thin", i)] = (psi_group, X_b)

# X_train["isdefault_1y"] = y_train
# dr = (
#     X_train[["isdefault_1y"]].groupby("isdefault_1y", observed=True).size()
#     / X_train.shape[0]
# )
# psi_save[("tims_thin", "isdefault_1y")] = (dr, [0, 1])

# COMMAND ----------

# with open(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/pkls/appl_v2_monitoring_pop_psi.pkl",
#     "wb",
# ) as f:  # open a text file
#     pickle.dump(psi_save, f)

# COMMAND ----------

def stability_calc(base_dat, new_dat, var):
    # sum of (init % - new % * ln ( init % / new % ) )
    pass
