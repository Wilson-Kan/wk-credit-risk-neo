# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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
        mth_tot_fXFeeRevenue,
        cvs_date,
        openedAt_mt,
        channel,
        source,
        productName,
        productTypeName,
        cardType,
        originalCreditScoreBand,
        currentCreditScoreBand,
        productCount,
        activeProductCounts,
        creditFacility,
        rn,
        avg_bal,
        max_bal,
        max_utilization,
        mthend_utilization,
        mthend_bal,
        creditLimitIncreaseOffer,
        creditLimitIncreaseRequested,
        creditLimitIncreaseDeclined,
        creditLimitIncreaseType,
        incrementalCreditLimitOfferAmount,
        changeCreditLimit,
        cumulativeCLIOffersExtended,
        creditLimit,
        availableCreditLimit,
        purchaseAPR,
        latestStatementPaymentDueDate,
        latestStatementMinimumPaymentAmountDollars,
        latestStatementNewBalanceAmountDollars,
        customerWebLoginsLast30D,
        customerOtherLoginsLast30D,
        hasBalanceProtection,
        mth_tot_cashAdvanceVolume,
        mth_tot_grossPurchaseVolume,
        mth_tot_netPurchaseVolume,
        mth_tot_grossRefundVolume,
        mth_tot_netInternationalPurchaseVolume,
        mth_tot_rewardCashOutVolume,
        mth_tot_interchangeRevenue,
        mth_tot_grossInterestRevenue,
        mth_tot_numberOfInternalPaymentTransactions,
        mth_tot_internationalCardPurchaseCount,
        mth_tot_rewardDollarsEarned,
        mth_tot_feeRevenue,
        mth_tot_cashAdvanceCount,
        tu_num_90d_6m_bc_BC77,
        cumulativeCLDOffersApplied,
        creditLimitIncreaseAccepted	)
    FROM
      neo_views_credit_risk.wk_utilization_v1_data
      where brand = "HBC"
  """
)

distinct_aid = pyspark_df.select("accountId").distinct()

train, test = distinct_aid.randomSplit(weights=[0.4, 0.6], seed=451995)

full_train_df = pyspark_df.join(train.select("accountId"), ["accountId"], "inner")

full_test_df = pyspark_df.join(test.select("accountId"), ["accountId"], "inner")


full_train_df_t_trans = full_train_df.withColumn(
    "t", F.floor(F.lit(24) * F.rand(703157)) + 1
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

display(model_df.groupBy("brand", "t").count().orderBy("t"))

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np  # Import numpy

# Convert the Spark DataFrame to Pandas DataFrame for plotting
model_df_pd = (
    model_df[["brand", "referenceDate", "target_util"]]
    .groupby(["brand", "referenceDate"])
    .mean("target_util")
    .toPandas()
)

# Convert referenceDate from string to datetime for proper plotting
model_df_pd["referenceDate"] = pd.to_datetime(model_df_pd["referenceDate"])

# Sort the DataFrame for plotting
model_df_pd.sort_values(by=["brand", "referenceDate"], inplace=True)

# Plotting
plt.figure(figsize=(12, 6))
for brand in model_df_pd["brand"].unique():
    subset = model_df_pd[model_df_pd["brand"] == brand]
    # Convert to numpy arrays before plotting
    dates = np.array(subset["referenceDate"])
    utils = np.array(subset["avg(target_util)"])
    plt.plot(dates, utils, label=brand)

plt.xlabel("Reference Date")
plt.ylabel("Target Utilization")
plt.title("Target Utilization by Brand over Time")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

model_hbc_temp_df = model_df.toPandas()

# COMMAND ----------

model_hbc_df = model_hbc_temp_df.drop(
    columns=["accountId", "future_date", "brand", "referenceDate"]
)

# COMMAND ----------

model_hbc_df["mth_tot_rewardsCost"] = model_hbc_df["mth_tot_rewardsCost"].astype(float)
model_hbc_df["avg_utilization"] = model_hbc_df["avg_utilization"].astype(float)
model_hbc_df["target_util"] = model_hbc_df["target_util"].astype(float)
model_hbc_df["latestStatementPastDueAmountDollars"] = model_hbc_df[
    "latestStatementPastDueAmountDollars"
].astype(float)

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

# Assuming 'target_util' is the target variable and exists in 'model_non_hbc_df'
X = model_hbc_df.drop(columns=["target_util"])
y = model_hbc_df["target_util"]

# Convert categorical variables to dummy variables
X = pd.get_dummies(X, drop_first=True)
X = X.fillna(0)

# Initialize and train the RandomForestRegressor
rf = RandomForestRegressor(n_estimators=25, random_state=42)
rf.fit(X, y)

# Predict on the test set
# y_pred = rf.predict(X_test)

# Calculate the mean squared error
# roc = roc_auc_score(y_test, y_pred)
# print(f"Gini: {2 * roc - 1}")

# Feature importance
feature_importances = pd.DataFrame(
    rf.feature_importances_, index=X.columns, columns=["importance"]
).sort_values("importance", ascending=False)

feature_importances.to_csv(
    "/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/feature_importances_hbc.csv"
)

feature_importances

# COMMAND ----------

from sklearn.inspection import permutation_importance

# Perform permutation importance
perm_importance = permutation_importance(rf, X, y, n_repeats=10, random_state=42)

# Organize results into a DataFrame
perm_importance_df = pd.DataFrame(
    {
        "feature": X.columns,
        "importance_mean": perm_importance.importances_mean,
        "importance_std": perm_importance.importances_std,
    }
)

# Sort by importance
perm_importance_df = perm_importance_df.sort_values(
    by="importance_mean", ascending=False
)

perm_importance_df.to_csv(
    "/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/perm_importance_df_hbc.csv"
)

perm_importance_df
