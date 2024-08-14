# Databricks notebook source
from xgboost import XGBClassifier
import xgboost as xgb
bst = XGBClassifier()
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_thick.json")
print("neo thick", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_thin.json")
print("neo thin", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/neo_subprime.json")
print("neo subprime", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/tims_thick_no_sp.json")
print("tims thick", bst.best_iteration)
bst.load_model("/Workspace/Users/wilson.kan@neofinancial.com/ApplicationV2/SaveModels/tims_thin_no_sp.json")
print("tims thin", bst.best_iteration)

# COMMAND ----------


