# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import numpy as np

spark = SparkSession.builder.getOrCreate()

pyspark_df = spark.sql(
    """
    SELECT
      *
      EXCEPT (subProductName,
        closedAt_mt,
        chargeOffCategory,
        chargedOffReason,
        multiCreditFlag,
        interestRate,
        moneyAccountBalanceDollars,
        collateralAmount,
        tu_worst_cur_rate_AM04,
        tu_mth_since_worst_rate_AM216,
        tu_mth_since_delq_AM36,
        tu_num_30d_12m_AM41,
        tu_num_60d_12m_AM42,
        tu_num_90d_12m_AM43,
        tu_num_60d_ever_AM44,
        tu_num_90d_ever_AM57,
        tu_tot_pastdue_AM84,
        tu_num_obc_5070_BC143,
        tu_num_obc_550_BC142,
        tu_num_obc_7090_BC144,
        tu_num_obc_90p_BC145,
        tu_mth_since_delq_bc_BC36,
        tu_num_30d_6m_bc_BC75,
        tu_num_60d_6m_bc_BC76,
        tu_num_30d_12m_bc_BC78,
        tu_num_60d_12m_bc_BC79,
        tu_num_90d_12m_bc_BC80,
        tu_tot_pastdue_bc_BC84,
        tu_mth_since_30delq_bc_BC85,
        tu_mth_since_60delq_bc_BC86,
        tu_num_obc_BC91,
        tu_num_abr_BR02,
        tu_tot_pastdue_br_BR84,
        tu_num_inq_6m_GO06,
        tu_mth_since_inq_GO07,
        tu_perc_never_delq_GO11,
        tu_num_inq_3m_no_util_ins_fuel_GO141,
        tu_num_dedup_inq_6m_GO148,
        tu_num_dedup_inq_1m_GO149,
        tu_num_inq_1m_GO15,
        tu_num_dedup_inq_3m_GO151,
        tu_num_dedup_inq_12m_GO152,
        tu_num_inq_3m_GO17,
        tu_num_inq_12m_GO21,
        tu_num_co_36m_GO80,
        tu_num_co_36m_bal200p_GO81,
        tu_num_trad_co_36m_GO83,
        tu_deceased_GO91,
        tu_mthly_pymt_ins_IN60,
        tu_tot_pastdue_ins_IN84,
        tu_mthly_pymt_mtg_MC60,
        tu_num_coll_PR09,
        tu_amt_coll_PR10,
        tu_num_coll_36m_bal250p_PR100,
        tu_tot_bal_coll_PR11,
        tu_num_unsat_prop_PR116,
        tu_num_sat_prop_PR117,
        tu_mth_coll_bal_200p_PR119,
        tu_num_unpaid_coll_24m_500p_PR120,
        tu_num_sat_prop_12m_PR123,
        tu_num_disc_bkr_12m_PR124,
        tu_num_garn_PR14,
        tu_num_forecl_PR15,
        tu_num_dero_pub_rec_PR21,
        tu_mth_dero_pub_rec_PR22,
        tu_num_coll_bal1000p_PR30,
        tu_num_coll_bal500p_PR41,
        tu_num_coll_bal100p_PR42,
        tu_num_coll_bal0p_PR43,
        tu_num_undisc_bkr_PR44,
        tu_num_disc_brk_PR45,
        tu_num_ord_pymt_debt_PR46,
        tu_num_bkr_prop_PR47,
        tu_num_col_500p_unpaid_PR51,
        tu_num_col_500p_paid_PR52,
        tu_num_col_250p_unpaid_24m_PR68,
        tu_num_paid_col_500p_11m_PR69,
        tu_num_paid_col_251p_11m_PR70,
        tu_num_coll_200p_3y_PR73,
        tu_num_unsat_judg_500p_24m_PR74,
        tu_num_unpaid_coll_24m_200p_PR75,
        tu_num_coll_250p_unpaid_36m_PR95,
        tu_num_coll_unpaid_36m_1000p_PR97,
        tu_num_coll_paid_36m_1000p_PR98,
        tu_num_are_trad_RE02,
        tu_num_re_trad_3m_RE05,
        tu_num_re_trad_6m_RE06,
        tu_num_re_trad_12m_RE07,
        tu_num_re_trad_24m_RE09,
        tu_num_re_trad_bal0p_RE29,
        tu_num_re_trad_bal5000p_RE37,
        tu_num_re_trad_bal1000p_RE38,
        tu_num_re_30dpd_12m_RE41,
        tu_num_re_60dpd_12m_RE42,
        tu_num_re_90dpd_12m_RE43,
        tu_num_re_30dpd_6m_RE75,
        tu_num_re_60dpd_6m_RE76,
        tu_num_re_90dpd_6m_RE77,
        tu_num_re_30dpd_24m_RE81,
        tu_num_re_60dpd_24m_RE82,
        tu_num_re_90dpd_24m_RE83,
        tu_tot_re_pastdue_RE84,
        tu_num_arr_RR02,
        tu_mthly_pymt_rr_RR60,
        tu_max_amt_hccl_arr_RR62,
        tu_tot_rr_pastdue_RR84,
        tu_num_orr_RR91,
        tu_mthly_pymt_sd_SD60,
        tu_mthly_pymt_sl_SL60,
        mth_tot_prepaidTransactions,
        mth_tot_billPayTransactions,
        mth_tot_grossDebitVolume,
        mth_tot_grossCreditVolume,
        mth_tot_etransferDebitVolume,
        mth_tot_etransferCreditVolume,
        mth_tot_eftDebitVolume,
        mth_tot_eftCreditVolume,
        mth_tot_billPaymentDebitVolume,
        mth_tot_billPaymentCreditVolume,
        mth_tot_prepaidPurchaseVolume,
        mth_tot_netPrepaidPurchaseVolume,
        mth_tot_prepaidPurchaseCount,
        mth_tot_prepaidRefundVolume,
        mth_tot_prepaidRefundCount,
        mth_tot_prepaidInternationalPurchaseCount,
        mth_tot_fXFeeRevenue)
    FROM
      neo_views_credit_risk.wk_utilization_v1_data
      where brand != "HBC"
  """
)

distinct_aid = pyspark_df.select("accountId").distinct()

train, test = distinct_aid.randomSplit(weights=[0.6, 0.4], seed=726178)

full_train_df = pyspark_df.join(train.select("accountId"), ["accountId"], "inner")

full_test_df = pyspark_df.join(test.select("accountId"), ["accountId"], "inner")


full_train_df_t_trans = full_train_df.withColumn(
    "t", F.floor(F.lit(24) * F.rand(42180)) + 1
)

full_train_df_t_trans = full_train_df_t_trans.withColumn(
    "future_date",
    F.last_day(
        F.add_months(full_train_df_t_trans["referenceDate"], full_train_df_t_trans["t"])
    ),
)

print(
    f"all data: {pyspark_df.count()} \n train data: {full_train_df.count()} \n test data: {full_test_df.count()} \n train transform data: {full_train_df_t_trans.count()}"
)

future_join_df = full_train_df.select(
    "accountId",
    F.col("referenceDate").alias("future_date"),
    F.col("avg_utilization").alias("target_util"),
)

model_df = full_train_df_t_trans.join(
    future_join_df, ["accountId", "future_date"], "inner"
)

print(f"mod data: {model_df.count()}")

# COMMAND ----------

non_hbc_df = model_df.select(
    "avg_utilization",
    "tu_credit_score_CVSC100",
    "t",
    "monthOnBook",
    "tu_tot_open_bc_BC94",
    "tu_mth_on_file_GO14",
    "tu_ratio_tot_bal_hccl_bc_BC34",
    "customerLoginsLast30D",
    "referenceDate",
    "future_date",
    "target_util",
)

model_non_hbc_temp_df = non_hbc_df.toPandas()

# COMMAND ----------

model_non_hbc_temp_df["future_mob"] = (
    model_non_hbc_temp_df["t"] + model_non_hbc_temp_df["monthOnBook"]
)
model_non_hbc_temp_df["future_mth"] = pd.to_datetime(
    model_non_hbc_temp_df["future_date"]
).dt.month

model_non_hbc_temp_df["transform_t"] = 1 / model_non_hbc_temp_df["t"]
model_non_hbc_temp_df["transform_future_mob"] = 1 / model_non_hbc_temp_df["future_mob"]

model_non_hbc_temp_df = pd.get_dummies(
    model_non_hbc_temp_df, columns=["future_mth"], drop_first=True, dtype=int
)
model_non_hbc_temp_df.drop(
    columns=["future_date", "referenceDate", "monthOnBook"], inplace=True
)
model_non_hbc_temp_df.fillna(0, inplace=True)

# Convert columns with decimal.Decimal to float
columns_to_convert = [
    "tu_credit_score_CVSC100", "tu_tot_open_bc_BC94", "tu_mth_on_file_GO14",
    "tu_ratio_tot_bal_hccl_bc_BC34", "customerLoginsLast30D", "avg_utilization"
]
for column in columns_to_convert:
    model_non_hbc_temp_df[column] = model_non_hbc_temp_df[column].astype(float)

# Now perform your operations as before

model_non_hbc_temp_df["t_dep_tu_credit_score_CVSC100"] = (
    model_non_hbc_temp_df["tu_credit_score_CVSC100"] * model_non_hbc_temp_df["transform_t"]
)
model_non_hbc_temp_df["t_dep_tu_tot_open_bc_BC94"] = (
    model_non_hbc_temp_df["tu_tot_open_bc_BC94"] * model_non_hbc_temp_df["transform_t"]
)
model_non_hbc_temp_df["t_dep_tu_mth_on_file_GO14"] = (
    model_non_hbc_temp_df["tu_mth_on_file_GO14"] * model_non_hbc_temp_df["transform_t"]
)
model_non_hbc_temp_df["t_dep_tu_ratio_tot_bal_hccl_bc_BC34"] = (
    model_non_hbc_temp_df["tu_ratio_tot_bal_hccl_bc_BC34"]
    * model_non_hbc_temp_df["transform_t"]
)
model_non_hbc_temp_df["t_dep_customerLoginsLast30D"] = (
    model_non_hbc_temp_df["customerLoginsLast30D"] * model_non_hbc_temp_df["transform_t"]
)
model_non_hbc_temp_df["t_dep_avg_utilization"] = (
    model_non_hbc_temp_df["avg_utilization"] * model_non_hbc_temp_df["transform_t"]
)

model_non_hbc_temp_df["mob_dep_tu_credit_score_CVSC100"] = (
    model_non_hbc_temp_df["tu_credit_score_CVSC100"]
    * model_non_hbc_temp_df["transform_future_mob"]
)
model_non_hbc_temp_df["mob_dep_tu_tot_open_bc_BC94"] = (
    model_non_hbc_temp_df["tu_tot_open_bc_BC94"]
    * model_non_hbc_temp_df["transform_future_mob"]
)
model_non_hbc_temp_df["mob_dep_tu_mth_on_file_GO14"] = (
    model_non_hbc_temp_df["tu_mth_on_file_GO14"]
    * model_non_hbc_temp_df["transform_future_mob"]
)
model_non_hbc_temp_df["mob_dep_tu_ratio_tot_bal_hccl_bc_BC34"] = (
    model_non_hbc_temp_df["tu_ratio_tot_bal_hccl_bc_BC34"]
    * model_non_hbc_temp_df["transform_future_mob"]
)
model_non_hbc_temp_df["mob_dep_customerLoginsLast30D"] = (
    model_non_hbc_temp_df["customerLoginsLast30D"]
    * model_non_hbc_temp_df["transform_future_mob"]
)

model_non_hbc_temp_df.drop(
    columns=[
        "transform_t",
        "transform_future_mob",
        "t"
    ],
    inplace=True,
)

# COMMAND ----------

from sklearn.preprocessing import StandardScaler 
    
# define standard scaler 
scaler = StandardScaler() 
  
# transform data 
non_standardized_col = ["future_mob",
"future_mth_2",
"future_mth_3",
"future_mth_4",
"future_mth_5",
"future_mth_6",
"future_mth_7",
"future_mth_8",
"future_mth_9",
"future_mth_10",
"future_mth_11",
"future_mth_12"]

# Transform data
scaled_data = scaler.fit_transform(model_non_hbc_temp_df.drop(columns=["target_util"] + non_standardized_col))

# Convert the numpy array back to a DataFrame
scaled_df = pd.DataFrame(scaled_data, columns=model_non_hbc_temp_df.drop(columns=["target_util"] + non_standardized_col).columns)

# Concatenate the non-standardized columns back to the scaled DataFrame
model_non_hbc_df = pd.concat([scaled_df, model_non_hbc_temp_df[["target_util"] + non_standardized_col].reset_index(drop=True)], axis=1)

model_non_hbc_df.head()

# COMMAND ----------

from sklearn.linear_model import LinearRegression

# Assuming 'target_util' is the target variable and exists in 'model_non_hbc_df'
X = model_non_hbc_df.drop(columns=["target_util"])
y = model_non_hbc_df["target_util"]

reg = LinearRegression().fit(X, y)

# Calculate the mean squared error
# mse = mean_squared_error(y_test, y_pred)
# print(f"Mean Squared Error: {mse}")

reg.coef_

# Feature importance
# feature_importances = pd.DataFrame(
#     rf.feature_importances_, index=X_train.columns, columns=["importance"]
# ).sort_values("importance", ascending=False)

# feature_importances

# COMMAND ----------


