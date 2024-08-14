# Databricks notebook source
# MAGIC %pip install stats_can

# COMMAND ----------

from stats_can import StatsCan
sc = StatsCan(data_folder="/Workspace/Users/wilson.kan@neofinancial.com/StatsCan")

# COMMAND ----------

df = sc.vectors_to_df(["v2062815"])

# COMMAND ----------

df

# COMMAND ----------


