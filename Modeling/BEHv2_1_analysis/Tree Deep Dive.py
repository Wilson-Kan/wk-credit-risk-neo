# Databricks notebook source
from xgboost import XGBClassifier

beh_v21 = XGBClassifier()
beh_v21.load_model("/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Modeling/BEHv2_1_analysis/beh_v21.json")
# beh_v21.get_booster().dump_model("/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Modeling/BEHv2_1_analysis/beh_v21_detail.txt")

# COMMAND ----------

tree_data = beh_v21.get_booster()

# COMMAND ----------

td = tree_data.trees_to_dataframe()
print(td)

# COMMAND ----------

display(td)

td.to_csv('/Workspace/Repos/wilson.kan@neofinancial.com/wk-credit-risk-neo/Modeling/BEHv2_1_analysis/tree_data.csv')

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.tree import plot_tree
import xgboost as xgb

fig, ax = plt.subplots(figsize=(30, 30))
xgb.plot_tree(beh_v21,ax=ax)
plt.show()

# COMMAND ----------


