# Databricks notebook source
# MAGIC %sql
# MAGIC select
# MAGIC   beh.accountId,
# MAGIC   ea.userId,
# MAGIC   beh.referenceDate,
# MAGIC   beh.daysPastDueBucket,
# MAGIC   ea.originalCreditScore,
# MAGIC   ea.currentCreditScore,
# MAGIC   ea.originalCreditScore - ea.currentCreditScore as creditScoreDeterioration,
# MAGIC   GREATEST(
# MAGIC     0,(ea.originalCreditScore - ea.currentCreditScore)
# MAGIC   ) as creditScoreDeterioration_floored,
# MAGIC   ea.personalIncomeBand,
# MAGIC   case
# MAGIC     when ea.personalIncomeBand = '\$0 - \$20,000' then 10000
# MAGIC     when ea.personalIncomeBand = '\$20,000 - \$40,000' then 30000
# MAGIC     when ea.personalIncomeBand = '\$40,000 - \$60,000' then 50000
# MAGIC     when ea.personalIncomeBand = '\$60,000 - \$80,000' then 70000
# MAGIC     when ea.personalIncomeBand = '\$80,000 - \$100,000' then 90000
# MAGIC     when ea.personalIncomeBand = '\$100,000 - \$120,000' then 110000
# MAGIC     when ea.personalIncomeBand = '\$120,000 - \$140,000' then 130000
# MAGIC     when ea.personalIncomeBand = '\$140,000+' then 150000
# MAGIC     else 60000
# MAGIC   end as personalIncome,
# MAGIC   ea.currentFSA,
# MAGIC   ea.customerAttributionChannel,
# MAGIC   beh.bad_rate,
# MAGIC   beh.modelScore,
# MAGIC   beh.risk_flag
# MAGIC from
# MAGIC   neo_data_science_production.credit_risk_behavior_pd_v2_1 as beh
# MAGIC   left join neo_trusted_analytics.earl_account as ea on ea.accountId = beh.accountId
# MAGIC   and ea.referenceDate = beh.referenceDate
# MAGIC where
# MAGIC   beh.referenceDate > '2022-05-01'
# MAGIC   and beh.referenceDate <= '2023-05-31'
# MAGIC   and beh.daysPastDueBucket in (
# MAGIC     'B0. Current',
# MAGIC     'B1. 0 - 29 dpd',
# MAGIC     'B2. 30 - 59 dpd',
# MAGIC     'B3. 60 - 89 dpd'
# MAGIC   )

# COMMAND ----------

beh_score = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   a.accountId as accountId_t,
# MAGIC   a.target_pd,
# MAGIC   b.target_co
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       distinct accountId,
# MAGIC       1 as target_pd
# MAGIC     from
# MAGIC       neo_trusted_analytics.earl_account
# MAGIC     where
# MAGIC       referenceDate > '2022-05-01'
# MAGIC       and (
# MAGIC         chargedOffReason not in ('N/A')
# MAGIC         or daysPastDue >= 90
# MAGIC       )
# MAGIC   ) as a
# MAGIC   left join (
# MAGIC     select
# MAGIC       distinct accountId,
# MAGIC       1 as target_co
# MAGIC     from
# MAGIC       neo_trusted_analytics.earl_account
# MAGIC     where
# MAGIC       referenceDate > '2022-05-01'
# MAGIC       and chargedOffReason not in ('N/A')
# MAGIC   ) as b on a.accountId = b.accountId

# COMMAND ----------

target_table = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   FSA,
# MAGIC   `Average Income` as avg_inc_fsa
# MAGIC FROM
# MAGIC   dbt_dev_alex_karklins.census_income
# MAGIC where
# MAGIC   (
# MAGIC     FSA != 'R8A'
# MAGIC     OR `Prov/Terr` != 47
# MAGIC   )

# COMMAND ----------

fsa_inc = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   accountID,
# MAGIC   max(bunsen_score) as bunsen_score
# MAGIC from
# MAGIC   dbt_dev_alex_karklins.cdt_v2_base_co_v1
# MAGIC where
# MAGIC   bunsen_score is not null
# MAGIC group by
# MAGIC   accountID

# COMMAND ----------

bunsen = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   userid,
# MAGIC   probability_fraud
# MAGIC FROM
# MAGIC   neo_analytics_production.beaker_application_scores

# COMMAND ----------

beaker = _sqldf

# COMMAND ----------

beh_target = beh_score.join(
    target_table, beh_score.accountId == target_table.accountId_t, "left"
)
beh_beaker = beh_target.join(
    beaker, beh_score.userId == beaker.userid, "left"
)
beh_bunsen = beh_beaker.join(
    bunsen, beh_beaker.accountId == bunsen.accountID, "left"
)
model_me = beh_bunsen.join(fsa_inc, beh_target.currentFSA == fsa_inc.FSA, "left")
model_me = model_me.fillna(
    {
        "target_pd": "0",
        "target_co": "0",
        "customerAttributionChannel": "N/A",
        "creditScoreDeterioration": "0",
    }
)
model_me = model_me.drop("accountId_t", "daysPastDueBucket","accountID","userid")
model_me = model_me.withColumn(
    "inc_diff", model_me.personalIncome - model_me.avg_inc_fsa
)

# COMMAND ----------

model_me_df = model_me.toPandas()

# COMMAND ----------

from sklearn.metrics import roc_auc_score

X_base = model_me_df["bad_rate"]
y_pd = model_me_df["target_pd"]
y_co = model_me_df["target_co"]
base_gini_pd = 2 * roc_auc_score(y_pd, X_base) - 1
print("Beh Model Alone, pd target: ", base_gini_pd)
base_gini_co = 2 * roc_auc_score(y_co, X_base) - 1
print("Beh Model Alone, co target: ", base_gini_co)

# COMMAND ----------

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier


def train_tree(mod_df, var, outstr, rand_s):
    clf = DecisionTreeClassifier(min_samples_leaf=0.0005, random_state=rand_s)
    X = mod_df[["bad_rate", *var]]
    y_pd = mod_df["target_pd"]
    y_co = mod_df["target_co"]
    clf.fit(X, y_pd)
    res = clf.predict_proba(X)[:, 1]
    gini_pd = 2 * roc_auc_score(y_pd, res) - 1
    print(f"Tree: {outstr}, pd target: ", gini_pd)
    print(f"Tree: {outstr}, pd lift: ", gini_pd - base_gini_pd)
    clf.fit(X, y_co)
    res = clf.predict_proba(X)[:, 1]
    gini_co = 2 * roc_auc_score(y_co, res) - 1
    print(f"Tree: {outstr}, co target: ", gini_co)
    print(f"Tree: {outstr}, co lift: ", gini_co - base_gini_co)
    return [var, gini_pd, gini_co]


def train_forest(mod_df, var, outstr, rand_s):
    clf = RandomForestClassifier(
        n_estimators=25,
        min_samples_leaf=0.0005,
        max_features=1,
        bootstrap=True,
        random_state=rand_s,
        max_samples=0.8,
    )
    X = mod_df[["bad_rate", *var]]
    y_pd = mod_df["target_pd"]
    y_co = mod_df["target_co"]
    clf.fit(X, y_pd)
    res = clf.predict_proba(X)[:, 1]
    gini_pd = 2 * roc_auc_score(y_pd, res) - 1
    print(f"Forest: {outstr}, pd target: ", gini_pd)
    print(f"Forest: {outstr}, pd lift: ", gini_pd - base_gini_pd)
    clf.fit(X, y_co)
    res = clf.predict_proba(X)[:, 1]
    gini_co = 2 * roc_auc_score(y_co, res) - 1
    print(f"Forest: {outstr}, co target: ", gini_co)
    print(f"Forest: {outstr}, co lift: ", gini_co - base_gini_co)
    return [var, gini_pd, gini_co]

# COMMAND ----------

# train_tree(model_me_df, ["creditScoreDeterioration"], "Unfloored score deterioration", 250822)
# train_forest(model_me_df, ["creditScoreDeterioration"], "Unfloored score deterioration", 250822)

# COMMAND ----------

# train_tree(model_me_df, ["creditScoreDeterioration_floored"], "Floored score deterioration", 152535)
# train_forest(model_me_df, ["creditScoreDeterioration_floored"], "Floored score deterioration", 152535)

# COMMAND ----------

# model_me_df[['inc_diff']] = model_me_df[['inc_diff']].fillna(value=0)
# train_tree(model_me_df, ["inc_diff"], "Income Difference", 580188)
# train_forest(model_me_df, ["inc_diff"], "Income Difference", 580188)

# COMMAND ----------

# import pandas as pd

# X = model_me_df[["customerAttributionChannel"]]
# X_trans = pd.get_dummies(X)
# col_name = X_trans.columns.to_list()
# col_name.append("bad_rate")
# attribution_df = pd.concat([model_me_df, X_trans], axis=1)

# COMMAND ----------

# train_tree(attribution_df, col_name, "Customer attribution channel", 60676)
# train_forest(attribution_df, col_name, "Customer attribution channel", 60676)

# COMMAND ----------

# model_filtered = model_me_df.fillna(subset=["bunsen_score"])
model_me_df[['bunsen_score']] = model_me_df[['bunsen_score']].fillna(value=-1)
train_tree(model_me_df, ["bunsen_score"], "Bunsen", 174832)
train_forest(model_me_df, ["bunsen_score"], "Bunsen", 174832)

# COMMAND ----------

# model_filtered = model_me_df.dropna(subset=["probability_fraud"])
model_me_df[['probability_fraud']] = model_me_df[['probability_fraud']].fillna(value=0)
train_tree(model_me_df, ["probability_fraud"], "Beaker", 869407)
train_forest(model_me_df, ["probability_fraud"], "Beaker", 869407)

# COMMAND ----------


