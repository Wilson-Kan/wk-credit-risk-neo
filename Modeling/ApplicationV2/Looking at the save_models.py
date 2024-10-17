# Databricks notebook source
from xgboost import XGBClassifier
import xgboost as xgb
bst = XGBClassifier()
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_thick.json")
bst.get_booster().trees_to_dataframe().to_csv("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_thick.csv")
# print("neo thick", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_thin.json")
bst.get_booster().trees_to_dataframe().to_csv("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_thin.csv")
# print("neo thin", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_subprime.json")
bst.get_booster().trees_to_dataframe().to_csv("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_subprime.csv")
# print("neo subprime", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/tims_thick_no_sp.json")
bst.get_booster().trees_to_dataframe().to_csv("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/tims_thick_no_sp.csv")
# print("tims thick", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/tims_thin_no_sp.json")
bst.get_booster().trees_to_dataframe().to_csv("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/tims_thin_no_sp.csv")
# print("tims thin", bst.best_iteration)

# COMMAND ----------


