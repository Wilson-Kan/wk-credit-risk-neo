# Databricks notebook source
# MAGIC %pip install stats_can

# COMMAND ----------

from stats_can import StatsCan
from pyspark.sql import SparkSession
import pandas as pd

pd.DataFrame.iteritems = pd.DataFrame.items
spark = SparkSession.builder.getOrCreate()

sc = StatsCan(data_folder="/Workspace/Users/wilson.kan@neofinancial.com/StatsCan")

df = sc.vectors_to_df(
    [
        "v2062815",
        "v41690914",
        "v41690915",
        "v62305845",
        "v79311153",
        "v122530",
        "v122620",
        "v111955442",
    ]
)

data_names = [
    "Unemployment_Can",
    "CPI_all",
    "CPI_food",
    "GDP_can",
    "WeeklyEarning_can",
    "bankrate",
    "TSX_close",
    "HPI",
]

# COMMAND ----------

df.columns = data_names
df.reset_index(inplace=True)
up_df = spark.createDataFrame(df)
up_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hive_metastore.neo_views_credit_risk.wk_statscan_economic_data")
