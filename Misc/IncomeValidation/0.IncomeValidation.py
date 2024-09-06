# Databricks notebook source
start_from_pickle = True

# COMMAND ----------

# if not start_from_pickle:
#     pass
# %sql
# select
# *
# from
#   (
#     select
#       accountId,
#       customerId,
#       userId,
#       brand,
#       monthOnBook,x
#       monthOnBookCustomer,
#       monthOnBookProduct,
#       channel,
#       source,
#       daysPastDue,
#       productName,
#       subProductName,
#       personalIncomeBand,
#       ageBand,
#       employmentStatus,
#       currentCity,
#       currentFSA,
#       currentProvince,
#       originalCreditScore,
#       currentCreditScore,
#       paymentVolume,
#       grossPurchaseVolume,
#       rewardCashOutVolume,
#       cumulativeCreditAccountRevenue,
#       creditLimit,
#       creditFacility,
#       balanceProtectionEnrollment
#     from
#       neo_trusted_analytics.earl_account
#     where
#       referenceDate = '2024-08-31'
#       and personalIncomeBand is not null
#       and activeProductCounts > 0
#       and accountStatus = 'OPEN'
#       and isAccountChargedOff = false
#   ) as ea
#   left join (
#     select
#       *
#     from
#       (
#         select
#           user_id,
#           AM02 as all_trade_cnt,
#           AM167 as all_trade_util_75,
#           AM33 as all_trade_bal,
#           AM60 as all_trade_mthly,
#           BC02 as bankcard_cnt,
#           BC147 as bankcard_max_util,
#           BC33 as bankcard_bal,
#           MC60 as mort_mthly,
#           RE01 as rev_trade_cnt,
#           RE02 as rev_trade_act_cnt,
#           RE03 as rev_trade_sat_cnt,
#           RE336 as rev_trade_max_util,
#           RE35 as rev_trade_avg_bal,
#           RE37 as rev_trade_5000,
#           RE38 as rev_trade_1000,
#           row_number() over (
#             partition by user_id
#             order by
#               createdat desc
#           ) as rn
#         from
#           neo_raw_production.transunion_creditreport_creditvision
#       )
#     where
#       rn = 1
#   ) as tu on ea.userId = tu.user_id

# COMMAND ----------

if not start_from_pickle:
    df = _sqldf
    df = df.toPandas()

# COMMAND ----------

import pickle

if not start_from_pickle:
    with open(
        "/Workspace/Users/wilson.kan@neofinancial.com/IncomeValidation/pkls/income_dat.pkl",
        "wb",
    ) as f:  # open a text file
        pickle.dump(df, f)
else:
    with open(
        "/Workspace/Users/wilson.kan@neofinancial.com/IncomeValidation/pkls/income_dat.pkl",
        "rb",
    ) as f:  # Correctly opening the file in binary read mode
        df = pickle.load(f)

# COMMAND ----------

df.columns

# COMMAND ----------

# import plotly.express as px

# df_process = df.groupby(["personalIncomeBand", "employmentStatus"]).agg({"userId": "count", "grossPurchaseVolume": "sum","all_trade_mthly":"sum"}).reset_index()
# df_process
# fig = px.scatter(df_process, x="all_trade_mthly", y="grossPurchaseVolume",
# 	         size="userId", color="employmentStatus",
#                  hover_name="personalIncomeBand", log_y=True, size_max=60)
# fig.show()

# COMMAND ----------

import pandas as pd

def make_dummy(df, var, pref):
    temp_dummies = pd.get_dummies(
        df[var],
        prefix=pref,
        drop_first=True,
        dummy_na=True,
        dtype=int,
    )
    res_df = pd.concat([df, temp_dummies], axis=1)
    return res_df.drop([var], axis=1)

# COMMAND ----------

from sklearn.tree import DecisionTreeRegressor

dummy_list = [
    "brand",
    "channel",
    "productName",
    "subProductName",
    "personalIncomeBand",
    "ageBand",
    "employmentStatus",
    "currentProvince",
    "creditFacility",
]
dummy_pref = [
    "brand",
    "channel",
    "prod",
    "subProd",
    "income",
    "age",
    "employment",
    "prov",
    "facility",
]
model_df = df.copy().drop(
    ["accountId", "customerId", "userId", "currentCity", "currentFSA", "user_id", "source"],
    axis=1,
)
for i in range(len(dummy_list)):
    model_df = make_dummy(model_df, dummy_list[i], dummy_pref[i])
print(f"pre na drop: {model_df.size}")
model_df[
    [
        "daysPastDue",
        "paymentVolume",
        "grossPurchaseVolume",
        "rewardCashOutVolume",
        "creditLimit",
        "rev_trade_5000",
    ]
] = model_df[
    [
        "daysPastDue",
        "paymentVolume",
        "grossPurchaseVolume",
        "rewardCashOutVolume",
        "creditLimit",
        "rev_trade_5000",
    ]
].fillna(
    value=0
)
model_df.dropna(inplace=True)
print(f"post na drop: {model_df.size}")
y = model_df["income_h. $140,000+"] + model_df["income_g. $120,000 - $140,000"]
X = model_df.drop(["income_h. $140,000+", "income_g. $120,000 - $140,000","income_b. $20,000 - $40,000",	"income_c. $40,000 - $60,000",	"income_d. $60,000 - $80,000",	"income_e. $80,000 - $100,000",	"income_f. $100,000 - $120,000"], axis=1)
regressor = DecisionTreeRegressor(random_state = 0)  
regressor.fit(X, y) 

# COMMAND ----------

import matplotlib.pyplot as plt
importances = regressor.feature_importances_[:model_df.shape[1] if len(regressor.feature_importances_) > model_df.shape[1] else len(regressor.feature_importances_)]

# Visualize feature importances
plt.figure(figsize=(12, 6))
plt.bar(range(X.shape[1]), importances)
plt.xticks(range(X.shape[1]), X.columns, rotation=90)
plt.ylabel('Feature Imporance Scores')
plt.xlabel('Features')
plt.title('Feature Importances using Random Forest')
plt.show()

# COMMAND ----------

# from sklearn.ensemble import IsolationForest

# dummy_list = [
#     "brand",
#     "channel",
#     "source",
#     "productName",
#     "subProductName",
#     "personalIncomeBand",
#     "ageBand",
#     "employmentStatus",
#     "currentProvince",
#     "creditFacility",
# ]
# dummy_pref = [
#     "brand",
#     "channel",
#     "source",
#     "prod",
#     "subProd",
#     "income",
#     "age",
#     "employment",
#     "prov",
#     "facility",
# ]
# model_df = df.copy().drop(
#     ["accountId", "customerId", "userId", "currentCity", "currentFSA", "user_id"],
#     axis=1,
# )
# for i in range(len(dummy_list)):
#     model_df = make_dummy(model_df, dummy_list[i], dummy_pref[i])
# print(f"pre na drop: {model_df.size}")
# model_df[
#     [
#         "daysPastDue",
#         "paymentVolume",
#         "grossPurchaseVolume",
#         "rewardCashOutVolume",
#         "creditLimit",
#         "rev_trade_5000",
#     ]
# ] = model_df[
#     [
#         "daysPastDue",
#         "paymentVolume",
#         "grossPurchaseVolume",
#         "rewardCashOutVolume",
#         "creditLimit",
#         "rev_trade_5000",
#     ]
# ].fillna(
#     value=0
# )
# x = model_df.isna().sum()
# model_df.dropna(inplace=True)
# print(f"post na drop: {model_df.size}")
# ifm = IsolationForest(
#     n_estimators=20,
#     max_samples="auto",
#     contamination="auto",
#     max_features=1.0,
#     bootstrap=True,
#     n_jobs=None,
#     random_state=None,
#     verbose=1,
#     warm_start=False,
# ).fit(model_df)

# COMMAND ----------

# outlier = model_df[model_df['outlier'] != 1]
# outlier.to_csv("/Workspace/Users/wilson.kan@neofinancial.com/IncomeValidation/ifm_test.csv")

# COMMAND ----------

# from sklearn.inspection import DecisionBoundaryDisplay
# import matplotlib.pyplot as plt
# disp = DecisionBoundaryDisplay.from_estimator(
#     ifm,
#     model_df,
#     response_method="predict",
#     alpha=0.5,
# )
# disp.ax_.scatter(model_df[:, 0], model_df[:, 1], c=res, s=20, edgecolor="k")
# disp.ax_.set_title("Binary decision boundary \nof IsolationForest")
# handles, labels = scatter.legend_elements()
# plt.axis("square")
# plt.legend(handles=handles, labels=["outliers", "inliers"], title="true class")
# plt.show()
