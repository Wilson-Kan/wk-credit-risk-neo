# Databricks notebook source
start_from_pickle = False

# COMMAND ----------

from pyspark.sql import SparkSession

# all idx and RR04 creditScore producttypename credit facility
spark = SparkSession.builder.getOrCreate()

if not start_from_pickle:

    modeling = spark.sql(
        """
    select
      base.*,
      ci.`Average Income` as fsa_income,
      case
        when income > 0
        and fsa_income > 0 then income - ci.`Average Income`
        else -1000000
      end as income_gap
    from
      (
        select
          brand,
          last_day(createdAt) as month_end,
          a.userId,
          -- origin,
          AM02,
          AM04,
          AM07,
          AM167,
          AM21,
          AM216,
          AM29,
          AM33,
          AM34,
          AM36,
          AM41,
          AM42,
          AM43,
          AM44,
          AM57,
          AM60,
          AM84,
          AM91,
          AT01,
          AT02,
          AT07,
          AT21,
          AT29,
          AT33,
          AT34,
          AT36,
          AT60,
          AT84,
          BC02,
          BC04,
          BC141,
          BC142,
          BC143,
          BC144,
          BC145,
          BC147,
          BC148,
          BC21,
          BC33,
          BC34,
          BC36,
          BC60,
          BC62,
          BC75,
          BC76,
          BC77,
          BC78,
          BC79,
          BC80,
          BC84,
          BC85,
          BC86,
          BC91,
          BC94,
          BR02,
          BR04,
          BR60,
          BR62,
          BR84,
          BR91,
          GO06,
          GO07,
          GO11,
          GO14,
          GO141,
          GO148,
          GO149,
          GO15,
          GO151,
          GO152,
          GO17,
          GO21,
          GO26,
          GO80,
          GO81,
          GO83,
          GO91,
          -- IDXBE01,
          -- IDXBE02,
          -- IDXBE03,
          -- IDXBE04,
          -- IDXBE05,
          -- IDXBE06,
          -- IDXBE07,
          -- IDXBE08,
          -- IDXBE09,
          -- IDXBE10,
          -- IDXBE11,
          -- IDXBE12,
          -- IDXBE13,
          -- IDXBE14,
          -- IDXBE15,
          -- IDXBE16,
          -- IDXBE17,
          -- IDXBE18,
          -- IDXBE19,
          -- IDXBE21,
          -- IDXBE22,
          -- IDXBE23,
          -- IDXBE24,
          -- IDXBE26,
          -- IDXBE27,
          -- IDXBE28,
          -- IDXBE30,
          -- IDXBE31,
          -- IDXBE35,
          -- IDXBE36,
          -- IDXBE38,
          -- IDXBE39,
          -- IDXBE40,
          -- IDXBE42,
          -- IDXBE43,
          -- IDXBE44,
          -- IDXBE45,
          -- IDXBE46,
          -- IDXBE47,
          -- IDXBE48,
          -- IDXBE49,
          -- IDXBE50,
          -- IDXBE51,
          -- IDXBE52,
          -- IDXBE53,
          -- IDXCF191,
          -- IDXCF193,
          -- IDXCF194,
          -- IDXCF237,
          -- IDXCF239,
          -- IDXFR01,
          -- IDXFR02,
          -- IDXFR03,
          -- IDXFR04,
          -- IDXFR05,
          -- IDXFR06,
          -- IDXFR07,
          -- IDXFR08,
          -- IDXFR09,
          -- IDXFR10,
          -- IDXFR100,
          -- IDXFR101,
          -- IDXFR102,
          -- IDXFR103,
          -- IDXFR104,
          -- IDXFR105,
          -- IDXFR106,
          -- IDXFR107,
          -- IDXFR108,
          -- IDXFR109,
          -- IDXFR11,
          -- IDXFR110,
          -- IDXFR111,
          -- IDXFR112,
          -- IDXFR113,
          -- IDXFR114,
          -- IDXFR115,
          -- IDXFR116,
          -- IDXFR117,
          -- IDXFR118,
          -- IDXFR12,
          -- IDXFR122,
          -- IDXFR125,
          -- IDXFR13,
          -- IDXFR130,
          -- IDXFR131,
          -- IDXFR136,
          -- IDXFR138,
          -- IDXFR139,
          -- IDXFR14,
          -- IDXFR146,
          -- IDXFR15,
          -- IDXFR153,
          -- IDXFR16,
          -- IDXFR162,
          -- IDXFR169,
          -- IDXFR17,
          -- IDXFR172,
          -- IDXFR173,
          -- IDXFR174,
          -- IDXFR176,
          -- IDXFR18,
          -- IDXFR184,
          -- IDXFR187,
          -- IDXFR188,
          -- IDXFR19,
          -- IDXFR20,
          -- IDXFR205,
          -- IDXFR206,
          -- IDXFR207,
          -- IDXFR208,
          -- IDXFR209,
          -- IDXFR21,
          -- IDXFR210,
          -- IDXFR211,
          -- IDXFR212,
          -- IDXFR213,
          -- IDXFR214,
          -- IDXFR215,
          -- IDXFR216,
          -- IDXFR217,
          -- IDXFR218,
          -- IDXFR219,
          -- IDXFR22,
          -- IDXFR220,
          -- IDXFR221,
          -- IDXFR222,
          -- IDXFR223,
          -- IDXFR224,
          -- IDXFR225,
          -- IDXFR226,
          -- IDXFR227,
          -- IDXFR228,
          -- IDXFR229,
          -- IDXFR23,
          -- IDXFR230,
          -- IDXFR231,
          -- IDXFR232,
          -- IDXFR233,
          -- IDXFR234,
          -- IDXFR235,
          -- IDXFR236,
          -- IDXFR24,
          -- IDXFR25,
          -- IDXFR26,
          -- IDXFR27,
          -- IDXFR28,
          -- IDXFR29,
          -- IDXFR30,
          -- IDXFR31,
          -- IDXFR32,
          -- IDXFR33,
          -- IDXFR34,
          -- IDXFR35,
          -- IDXFR36,
          -- IDXFR37,
          -- IDXFR38,
          -- IDXFR39,
          -- IDXFR40,
          -- IDXFR41,
          -- IDXFR42,
          -- IDXFR43,
          -- IDXFR44,
          -- IDXFR45,
          -- IDXFR46,
          -- IDXFR47,
          -- IDXFR48,
          -- IDXFR49,
          -- IDXFR50,
          -- IDXFR51,
          -- IDXFR52,
          -- IDXFR53,
          -- IDXFR54,
          -- IDXFR55,
          -- IDXFR56,
          -- IDXFR57,
          -- IDXFR58,
          -- IDXFR59,
          -- IDXFR60,
          -- IDXFR61,
          -- IDXFR62,
          -- IDXFR63,
          -- IDXFR64,
          -- IDXFR65,
          -- IDXFR66,
          -- IDXFR67,
          -- IDXFR68,
          -- IDXFR69,
          -- IDXFR70,
          -- IDXFR71,
          -- IDXFR72,
          -- IDXFR73,
          -- IDXFR74,
          -- IDXFR75,
          -- IDXFR76,
          -- IDXFR77,
          -- IDXFR78,
          -- IDXFR79,
          -- IDXFR80,
          -- IDXFR81,
          -- IDXFR82,
          -- IDXFR83,
          -- IDXFR84,
          -- IDXFR85,
          -- IDXFR86,
          -- IDXFR87,
          -- IDXFR88,
          -- IDXFR89,
          -- IDXFR90,
          -- IDXFR91,
          -- IDXFR92,
          -- IDXFR93,
          -- IDXFR94,
          -- IDXFR95,
          -- IDXFR96,
          -- IDXFR97,
          -- IDXFR98,
          -- IDXFR99,
          -- IDXID01,
          -- IDXID03,
          -- IDXID04,
          -- IDXID05,
          -- IDXID06,
          -- IDXID07,
          -- IDXID09,
          -- IDXID10,
          -- IDXID11,
          -- IDXID12,
          -- IDXID13,
          -- IDXID14,
          -- IDXID15,
          -- IDXID17,
          -- IDXID18,
          -- IDXID19,
          -- IDXID20,
          -- IDXID21,
          -- IDXID23,
          -- IDXID24,
          -- IDXID25,
          -- IDXID26,
          -- IDXID27,
          -- IDXID28,
          -- IDXID30,
          -- IDXID32,
          -- IDXID33,
          -- IDXID34,
          -- IDXID35,
          -- IDXID36,
          -- IDXID37,
          -- IDXSF190,
          -- IDXSF191,
          -- IDXSF192,
          -- IDXSF193,
          -- IDXSF194,
          -- IDXSF197,
          -- IDXSF202,
          -- IDXSF237,
          -- IDXSF238,
          -- IDXSF240,
          -- IDXSF241,
          -- IDXSF244,
          IN04,
          IN60,
          IN84,
          MC60,
          PR09,
          PR10,
          PR100,
          PR11,
          PR116,
          PR117,
          PR119,
          PR120,
          PR123,
          PR124,
          PR14,
          PR15,
          PR21,
          PR22,
          PR30,
          PR41,
          PR42,
          PR43,
          PR44,
          PR45,
          PR46,
          PR47,
          PR50,
          PR51,
          PR52,
          PR68,
          PR69,
          PR70,
          PR73,
          PR74,
          PR75,
          PR95,
          PR97,
          PR98,
          RE01,
          RE02,
          RE03,
          RE04,
          RE05,
          RE06,
          RE07,
          RE09,
          RE28,
          RE29,
          RE33,
          RE336,
          RE34,
          RE35,
          RE37,
          RE38,
          RE41,
          RE42,
          RE43,
          RE60,
          RE61,
          RE62,
          RE75,
          RE76,
          RE77,
          RE81,
          RE82,
          RE83,
          RE84,
          RE91,
          RR02,
          -- RR04,
          RR60,
          RR62,
          RR84,
          RR91,
          SD60,
          SL60,
          monthlyHousingCostCents,
          housingStatus,
          case
            when personalIncomeBand = 'a.\$0 - \$20,000,' then 10000
            when personalIncomeBand = 'b.\$20,000 - \$40,000' then 30000
            when personalIncomeBand = 'c.\$40,000 - \$60,000' then 50000
            when personalIncomeBand = 'd.\$60,000 - \$80,000' then 70000
            when personalIncomeBand = 'e.\$80,000 - \$100,000' then 90000
            when personalIncomeBand = 'f.\$100,000 - \$120,000' then 110000
            when personalIncomeBand = 'g.\$120,000 - \$140,000' then 130000
            when personalIncomeBand = 'h.\$140,000+' then 150000
            else -10000
          end as income,
          originalCreditScore,
          cast(
            a.transunionSoftCreditCheckResult.creditScore as int
          ) as creditScore,
          case
            when creditScore < 640 then 1
            else 0
          end as subprime,
          case
            when (
              AT01 <= 0
              and GO14 <= 24
              and subprime = 0
            ) then 1
            else 0
          end as thin,
          substring(userInformation.physicalAddress.postal, 1, 3) as FSA,
          CAST(balance AS float) as balance,
          CAST(creditLimit AS float) as creditLimit,
          CAST(dateLastActivity AS float) as dateLastActivity,
          CAST(dateOpened AS float) as dateOpened,
          CAST(dateRevised AS float) as dateRevised,
          --CAST(frequency AS float) as frequency,
          CAST(highCredit AS float) as highCredit,
          --CAST(joint AS float) as joint,
          CAST(maskedAccountNumber AS float) as maskedAccountNumber,
          CAST(memberCode AS float) as memberCode,
          CAST(memberName AS float) as memberName,
          CAST(monthsReviewed AS float) as monthsReviewed,
          CAST(mop AS float) as mop,
          CAST(pastDue AS float) as pastDue,
          CAST(payment AS float) as payment,
          --CAST(paymentPattern AS float) as paymentPattern,
          --CAST(paymentPatternStartDate AS float) as paymentPatternStartDate,
          CAST(plus30 AS float) as plus30,
          CAST(plus60 AS float) as plus60,
          CAST(plus90 AS float) as plus90,
          isdefault_1y
        from
          neo_views_credit_risk.wk_feature_and_target_w_hc as a
          left join (
            select
              distinct userId,
              personalIncomeBand,
              originalCreditScore
            from
              neo_trusted_analytics.earl_account
          ) as b on a.userId = b.userId
        where
          brand = 'NEO'
          and decision = 'APPROVED'
          and type = 'STANDARD'
      ) as base
      left join dbt_dev_alex_karklins.census_income as ci on base.fsa = ci.fsa
    """
    )

# COMMAND ----------

from pyspark.sql.functions import *

if not start_from_pickle:
    amount_missing_df = modeling.select(
        [
            (count(when(col(c).isNull(), c)) / count(lit(1))).alias(c)
            for c in modeling.columns
        ]
    )

    display(amount_missing_df)
    modeling_df = modeling.toPandas()
    idx = 0
    for i in modeling_df.dtypes:
        print(modeling_df.columns[idx], i)
        idx += 1

# COMMAND ----------

import pandas as pd

house_dummies = pd.get_dummies(
    modeling_df["housingStatus"],
    prefix="houseStat",
    drop_first=True,
    dummy_na=True,
    dtype=int,
)
modeling_dummy_df = pd.concat([modeling_df, house_dummies], axis=1)
modeling_dummy_df.drop(
    ["brand", "userId", "FSA", "housingStatus"], axis=1, inplace=True
)
modeling_dummy_df.head()

# COMMAND ----------

import pickle

if not start_from_pickle:
    with open(
        "/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Modeling/ApplicationV2/modeling_ready_hc.pkl",
        "wb",
    ) as f:  # open a text file
        pickle.dump(modeling_dummy_df, f)
else:
    with open(
        "/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Modeling/ApplicationV2/modeling_ready_hc.pkl",
        "rb",
    ) as f:  # Correctly opening the file in binary read mode
        modeling_dummy_df = pickle.load(f)

# COMMAND ----------

import numpy as np

modeling_oot = modeling_dummy_df[
    modeling_dummy_df["month_end"].isin(
        [np.datetime64("2023-06-30"), np.datetime64("2023-07-31")]
    )
]
modeling_intime = modeling_dummy_df[
    modeling_dummy_df["month_end"] < np.datetime64("2023-06-30")
]

# COMMAND ----------

from xgboost import XGBClassifier
import xgboost as xgb
from sklearn.model_selection import train_test_split

# dropping duplicated features
X_train, X_test, y_train, y_test = train_test_split(
    modeling_intime.drop(["month_end", "isdefault_1y", "originalCreditScore", "GO17"], axis=1),
    modeling_intime["isdefault_1y"],
    test_size=0.2,
)
# create model instance
bst = XGBClassifier(
    n_estimators=50, max_depth=6, colsample_bytree = 0.65, subsample = 0.5, objective="binary:logistic", random_state=601715
)
# fit model
bst.fit(X_train, y_train)
xgb.plot_importance(bst)
# make predictions
# preds = bst.predict(X_test)

# COMMAND ----------

feature_imp = []

for i in range(len(bst.feature_importances_)):
    feature_imp.append((bst.feature_names_in_[i], bst.feature_importances_[i]))

feature_imp.sort(key=lambda tup: tup[1], reverse=True)
feature_imp

# COMMAND ----------

top_set = []
for i in range(20):
  top_set.append(feature_imp[i][0])
# create model instance
bst = XGBClassifier(
    n_estimators=50, max_depth=6, colsample_bytree = 0.65, subsample = 0.5, objective="binary:logistic", random_state=517218
)
# fit model
bst.fit(X_train[top_set], y_train)
xgb.plot_importance(bst)

# COMMAND ----------

from sklearn.metrics import roc_auc_score

x_train_scr = bst.predict_proba(X_train[top_set])
x_test_scr = bst.predict_proba(X_test[top_set])
x_oot_scr = bst.predict_proba(modeling_oot[top_set])
print("train", roc_auc_score(y_train, x_train_scr[:, 1]))
print("test", roc_auc_score(y_test, x_test_scr[:, 1]))
print("oot", roc_auc_score(modeling_oot["isdefault_1y"], x_oot_scr[:, 1]))

# COMMAND ----------

sp = modeling_dummy_df[modeling_dummy_df["subprime"] == 1]
thin = modeling_dummy_df[modeling_dummy_df["thin"] == 1]
thick = modeling_dummy_df[
    modeling_dummy_df["subprime"] + modeling_dummy_df["thin"] == 0
]
x_subprime_scr = bst.predict_proba(sp[top_set])
x_thin_scr = bst.predict_proba(thin[top_set])
x_thick_scr = bst.predict_proba(thick[top_set])
print("subprime", roc_auc_score(sp["isdefault_1y"], x_subprime_scr[:, 1]))
print("thin", roc_auc_score(thin["isdefault_1y"], x_thin_scr[:, 1]))
print("thick", roc_auc_score(thick["isdefault_1y"], x_thick_scr[:, 1]))

# COMMAND ----------


