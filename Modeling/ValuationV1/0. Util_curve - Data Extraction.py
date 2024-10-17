# Databricks notebook source
# MAGIC %md
# MAGIC ###Utilization Modeling
# MAGIC Initial data processing is done [here](https://neofinancial-production.cloud.databricks.com/editor/queries/272741009473582?o=8838479919770272).

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

pyspark_df = spark.sql(
  """
    SELECT
      *
    FROM
      neo_views_credit_risk.wk_utilization_v1_data
    where brand != 'HBC' and monthOnBook <= 0
  """
)

pandas_nonhbc_df = pyspark_df.toPandas()

# pyspark_df = spark.sql(
#   """
#     SELECT
#       *
#     FROM
#       neo_views_credit_risk.wk_utilization_v1_data
#     where brand != 'HBC'
#   """
# )

# pandas_nonhbc_df = pyspark_df.toPandas()

# COMMAND ----------

# check different segments - brand, referenceDate, vintage, channel, source, monthOnBook
# restructure data for t/mob forward

# COMMAND ----------

import matplotlib.pyplot as plt

def plot_segment_util(df, x, val, seg, title):
  # x = [10,20,30,40,50]
  # y = [12,34,23,43,30]
  # x1 = [5,15,25,35,45]
  # y1 = [20,40,10,20,40]
  # plt.plot(x,y,label='A',color='m', marker='*', markersize=20)
  # plt.plot(x1,y1,label='B',color='g', marker='h', markersize=10)
  # plt.xlabel('x-axis')
  # plt.ylabel('y-axis')
  # plt.title('Customizing Line Chart')
  # plt.legend()
  # plt.show()
  pass

# COMMAND ----------

test

# COMMAND ----------


