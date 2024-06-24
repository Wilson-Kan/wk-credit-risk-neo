# Databricks notebook source
from xgboost import XGBClassifier
beh_v21 = XGBClassifier().load_model("/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Modeling/BEHv2_1_analysis/beh_v21.json")

# COMMAND ----------

beh_v21.s

# COMMAND ----------


