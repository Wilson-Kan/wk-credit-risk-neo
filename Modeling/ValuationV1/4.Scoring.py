# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date

pd.DataFrame.iteritems = pd.DataFrame.items

spark = SparkSession.builder.getOrCreate()

def data_pull(ishbc, snap_shot):
    if ishbc:
        brand_ind = ("HBC", "")
    else:
        brand_ind = ("NEO", "SIENNA", "CATHAY")

    pyspark_df = spark.sql(
        f"""
      select
    tu_info.*,
    bal_info.mth_tot_purchaseAndCashAdvanceCount,
    bal_info.avg_bal,
    case
      when tu_info.creditLimit > 0 then bal_info.avg_bal / tu_info.creditLimit
      else 0
    end as avg_utilization
  from
    (
      select
        *
      from
        (
          select
            a.accountId,
            a.brand,
            a.referenceDate,
            a.monthOnBook,
            a.creditLimit,
            a.cumulativeCreditAccountRevenue,
            a.customerLoginsLast30D,
            CAST (BC34 as float) as tu_ratio_tot_bal_hccl_bc_BC34,
            CAST (BC94 as float) as tu_tot_open_bc_BC94,
            CAST (GO14 as int) as tu_mth_on_file_GO14,
            CAST (CVSC100 as int) as tu_credit_score_CVSC100,
            row_number() over (
              partition by a.accountId,
              a.referenceDate
              order by
                to_date(b.credit_report_date, 'yyyyMMdd') desc
            ) as rn
          from
            neo_trusted_analytics.earl_account as a
            left join neo_raw_production.transunion_creditreport_creditvision as b on a.userId = b.user_id
            and a.referenceDate >= to_date(b.credit_report_date, 'yyyyMMdd')
          where
            a.referenceDate = TO_DATE('{snap_shot}', 'yyyy-MM-dd')
            and a.productName = "CREDIT"
            and a.subProductName = "UNSECURED"
            and a.closedAt_mt is null
            and accountStatus = "OPEN"
            and a.chargedOffAt_mt is null
            and a.brand in {brand_ind}
        )
      where
        rn = 1
    ) as tu_info
    left join (
      select
        accountId,
        '{snap_shot}' as referenceDate,
        avg(postedCreditAccountBalanceDollars) as avg_bal,
        sum (purchaseAndCashAdvanceCount) AS mth_tot_purchaseAndCashAdvanceCount
      from
        neo_trusted_analytics.earl_account
      where
        productName = "CREDIT"
        and subProductName = "UNSECURED"
        and closedAt_mt is null
        and accountStatus = "OPEN"
        and chargedOffAt_mt is null
        and brand in {brand_ind} and referenceDate between dateadd(day, -29, TO_DATE('{snap_shot}', 'yyyy-MM-dd')) and TO_DATE('{snap_shot}', 'yyyy-MM-dd')
      group by
        accountId
    ) as bal_info on tu_info.accountId = bal_info.accountId
    """
    )

    # zero_list = [
    #     "future_mth_2",
    #     "future_mth_3",
    #     "future_mth_4",
    #     "future_mth_5",
    #     "future_mth_6",
    #     "future_mth_7",
    #     "future_mth_8",
    #     "future_mth_9",
    #     "future_mth_10",
    #     "future_mth_11",
    #     "future_mth_12",
    # ]

    # for z in zero_list:
    #     pyspark_df = pyspark_df.withColumn(z, F.lit(0))

    df = pyspark_df.toPandas()
    return df

# COMMAND ----------

def data_tranform(df, t):

    df["t"] = t
    df["future_date"] = pd.to_datetime(df["referenceDate"]) + pd.DateOffset(months=t)

    df["future_mob"] = df["t"] + df["monthOnBook"]
    df["future_mth"] = pd.to_datetime(df["future_date"]).dt.month

    df["transform_t"] = 1 / df["t"]
    df["transform_future_mob"] = 1 / df["future_mob"]

    # df = pd.get_dummies(df, columns=["future_mth"], drop_first=True, dtype=int)
    # df.drop(columns=["future_date", "referenceDate", "monthOnBook"], inplace=True)
    df.fillna(0, inplace=True)

    df["tu_tot_open_bc_BC94"] = np.log(np.maximum(df["tu_tot_open_bc_BC94"], 1))
    df["cumulativeCreditAccountRevenue"] = np.log(
        np.maximum(df["cumulativeCreditAccountRevenue"], 1)
    )

    # Convert columns with decimal.Decimal to float
    columns_to_convert = [
        "avg_utilization",
        "tu_ratio_tot_bal_hccl_bc_BC34",
        "mth_tot_purchaseAndCashAdvanceCount",
        "tu_tot_open_bc_BC94",
        "tu_credit_score_CVSC100",
        "customerLoginsLast30D",
        "tu_mth_on_file_GO14",
    ]
    for column in columns_to_convert:
        df[column] = df[column].astype(float)

    transform_col = [
        "tu_credit_score_CVSC100",
        "tu_tot_open_bc_BC94",
        "tu_mth_on_file_GO14",
        "tu_ratio_tot_bal_hccl_bc_BC34",
        "customerLoginsLast30D",
        "tu_credit_score_CVSC100",
        "avg_utilization",
    ]

    for f in transform_col:
        df[f"t_dep_{f}"] = df[f] * df["transform_t"]
        df[f"mob_dep_{f}"] = df[f] * df["transform_future_mob"]

    for i in range(2,13):
        df[f"future_mth_{i}"] = 0
        if i == df["future_date"].unique()[0].month:
            df[f"future_mth_{i}"] = 1
            
    return df

# COMMAND ----------

import pickle

with open(
    r"/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/pkls/hbc_scalar_reg.pkl",
    "rb",
) as input_file:
    [shbc, rhbc] = pickle.load(input_file)
with open(
    r"/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/pkls/non_hbc_scalar_reg.pkl",
    "rb",
) as input_file:
    [snhbc, rnhbc] = pickle.load(input_file)

# COMMAND ----------

def score(ishbc, df, t):

  scaler = snhbc
  reg = rnhbc
  if ishbc:
    scaler = shbc
    reg = rhbc
  transform_me = df[list(scaler.feature_names_in_)]
  transformed = scaler.transform(transform_me)

  scaled_df = pd.DataFrame(
      transformed,
      columns=list(scaler.feature_names_in_)
  )

  join_l = df.drop(columns=list(scaler.feature_names_in_))
  join_l = pd.concat([join_l, scaled_df], axis=1)

  scored = join_l[list(reg.feature_names_in_)]
  return reg.predict(scored)

# COMMAND ----------

ishbc = True
snap_shot = "2023-11-11"
snap_pull = data_pull(ishbc, snap_shot)

res_out = snap_pull[["accountId", "referenceDate", "brand"]]

for t in range(1, 25):
  transformed = data_tranform(snap_pull, t)
  res_out[f"util_pred_{t}"] = score(ishbc, transformed, t)

res_out_spark = spark.createDataFrame(res_out)
res_out_spark.write.mode("overwrite").saveAsTable(f"neo_views_credit_risk.wk_util_forecast_hbc{snap_shot.translate(str.maketrans('', '', '-'))}")

# COMMAND ----------

ishbc = False
snap_shot = "2023-11-11"
snap_pull = data_pull(ishbc, snap_shot)

res_out = snap_pull[["accountId", "referenceDate", "brand"]]

for t in range(1, 25):
  transformed = data_tranform(snap_pull, t)
  res_out[f"util_pred_{t}"] = score(ishbc, transformed, t)

res_out_spark = spark.createDataFrame(res_out)
res_out_spark.write.mode("overwrite").saveAsTable(f"neo_views_credit_risk.wk_util_forecast_nonhbc{snap_shot.translate(str.maketrans('', '', '-'))}")

# COMMAND ----------

ishbc = True
snap_shot = "2022-11-11"
snap_pull = data_pull(ishbc, snap_shot)

res_out = snap_pull[["accountId", "referenceDate", "brand"]]

for t in range(1, 25):
  transformed = data_tranform(snap_pull, t)
  res_out[f"util_pred_{t}"] = score(ishbc, transformed, t)

res_out_spark = spark.createDataFrame(res_out)
res_out_spark.write.mode("overwrite").saveAsTable(f"neo_views_credit_risk.wk_util_forecast_hbc{snap_shot.translate(str.maketrans('', '', '-'))}")

# COMMAND ----------

ishbc = False
snap_shot = "2022-11-11"
snap_pull = data_pull(ishbc, snap_shot)

res_out = snap_pull[["accountId", "referenceDate", "brand"]]

for t in range(1, 25):
  transformed = data_tranform(snap_pull, t)
  res_out[f"util_pred_{t}"] = score(ishbc, transformed, t)

res_out_spark = spark.createDataFrame(res_out)
res_out_spark.write.mode("overwrite").saveAsTable(f"neo_views_credit_risk.wk_util_forecast_nonhbc{snap_shot.translate(str.maketrans('', '', '-'))}")

# COMMAND ----------


