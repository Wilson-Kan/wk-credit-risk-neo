# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark
import pyspark.sql.functions as func
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    FloatType,
    DecimalType,
    BooleanType
)

# COMMAND ----------

# def quantile_plot(df, col, ptile=5, isString=False, isDate=False, isBoolean=False):
#     temp_df = df.select("creditLimit", "avg_bal", col)
#     print(
#         f"{col} is running... isString: {isString}, isDate: {isDate}, isBoolean: {isBoolean}"
#     )
#     if isString:
#         res = temp_df.groupBy(col).agg(
#             func.sum("avg_bal").alias("bal"),
#             func.sum("creditLimit").alias("cl"),
#             func.count("avg_bal").alias("cnt"),
#         )
#         res = res.select(func.col(col).alias("quantile_group"), "bal", "cl", "cnt")
#         res = (
#             res.withColumn("min", func.lit(None))
#             .withColumn("max", func.lit(None))
#             .withColumn("feature", func.lit(col))
#         )
#     elif isDate:
#         temp_df = temp_df.withColumn(col, func.unix_timestamp(col))
#         w = Window.orderBy(col)
#         temp_df = temp_df.withColumn(
#             "quantile_group", func.floor(func.percent_rank().over(w) * ptile) + 1
#         )
#         temp_df = temp_df.withColumn(
#             "quantile_group",
#             func.when(temp_df.quantile_group == ptile + 1, ptile).otherwise(
#                 temp_df.quantile_group
#             ),
#         )
#         res = temp_df.groupBy("quantile_group").agg(
#             func.min(col).alias("min"),
#             func.max(col).alias("max"),
#             func.sum("avg_bal").alias("bal"),
#             func.sum("creditLimit").alias("cl"),
#             func.count("avg_bal").alias("cnt"),
#         )
#         res = res.withColumn("feature", func.lit(col))
#     elif isBoolean:
#         temp_df = temp_df.withColumn(col, func.when(temp_df[col], 1).otherwise(0))
#         w = Window.orderBy(col)
#         temp_df = temp_df.withColumn(
#             "quantile_group", func.floor(func.percent_rank().over(w) * ptile) + 1
#         )
#         temp_df = temp_df.withColumn(
#             "quantile_group",
#             func.when(temp_df.quantile_group == ptile + 1, ptile).otherwise(
#                 temp_df.quantile_group
#             ),
#         )
#         res = temp_df.groupBy("quantile_group").agg(
#             func.min(col).alias("min"),
#             func.max(col).alias("max"),
#             func.sum("avg_bal").alias("bal"),
#             func.sum("creditLimit").alias("cl"),
#             func.count("avg_bal").alias("cnt"),
#         )
#         res = res.withColumn("feature", func.lit(col))
#     else:
#         w = Window.orderBy(col)
#         temp_df = temp_df.withColumn(
#             "quantile_group", func.floor(func.percent_rank().over(w) * ptile) + 1
#         )
#         temp_df = temp_df.withColumn(
#             "quantile_group",
#             func.when(temp_df.quantile_group == ptile + 1, ptile).otherwise(
#                 temp_df.quantile_group
#             ),
#         )
#         res = temp_df.groupBy("quantile_group").agg(
#             func.min(col).alias("min"),
#             func.max(col).alias("max"),
#             func.sum("avg_bal").alias("bal"),
#             func.sum("creditLimit").alias("cl"),
#             func.count("avg_bal").alias("cnt"),
#         )
#         res = res.withColumn("feature", func.lit(col))
#     res = res.withColumn("util", res.bal / res.cl)
#     res = res.withColumn("isString", func.lit(isString))
#     return res

# spark = SparkSession.builder.getOrCreate()

# pyspark_df = spark.sql(
#     """
#     SELECT
#       *
#     FROM
#       neo_views_credit_risk.wk_utilization_v1_data
#   """
# )

# schema = StructType(
#     [
#         StructField("feature", StringType()),
#         StructField("quantile_group", StringType()),
#         StructField("min", DecimalType()),
#         StructField("max", DecimalType()),
#         StructField("bal", DecimalType()),
#         StructField("cl", DecimalType()),
#         StructField("cnt", LongType()),
#         StructField("util", FloatType()),
#         StructField("isString", BooleanType()),
#     ]
# )

# final_out = spark.createDataFrame([], schema)

# for k, v in pyspark_df.dtypes:
#     if k != "accountId":
#         if v == "string":
#             union_me = quantile_plot(pyspark_df, k, isString=True)
#         elif v == "date":
#             union_me = quantile_plot(pyspark_df, k, isDate=True)
#         elif v == "boolean":
#             union_me = quantile_plot(pyspark_df, k, isBoolean=True)
#         else:
#             union_me = quantile_plot(pyspark_df, k)
#         final_out = final_out.unionByName(union_me)

# final_out.toPandas().to_csv(
#     "/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/for_eda.csv"
# )

# COMMAND ----------

plot_me = pd.read_csv("/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/for_eda.csv")

for fea in plot_me['feature'].unique():
  print(fea)
  plot_spec = plot_me[plot_me['feature'] == fea][['quantile_group', 'cnt', 'util']]

  fig, ax = plt.subplots(figsize=(12,5))
  ax.set_title(fea)
  ax2 = ax.twinx()

  x_ticks = np.arange(len(plot_spec['quantile_group']))
  plot_spec.plot(x='quantile_group', y='cnt', kind='bar', ax=ax, alpha=0.2, color='black')
  ax.set_xticks(x_ticks)
  ax.set_xticklabels(plot_spec['quantile_group'], fontsize=8)
  ax.get_legend().remove()

  ax2.plot(x_ticks, plot_spec['util'].values, color='black')  # Convert to numpy array
  ax.set_ylabel('Count')
  ax2.set_ylabel('util')

  plt.tight_layout()
  plt.show()

# COMMAND ----------

def quantile_plot(df, col, ptile=5, isString=False, isDate=False, isBoolean=False):
    temp_df = df.select("creditLimit", "avg_bal", col)
    print(
        f"{col} is running... isString: {isString}, isDate: {isDate}, isBoolean: {isBoolean}"
    )
    if isString:
        res = temp_df.groupBy(col).agg(
            func.sum("avg_bal").alias("bal"),
            func.sum("creditLimit").alias("cl"),
            func.count("avg_bal").alias("cnt"),
        )
        res = res.select(func.col(col).alias("quantile_group"), "bal", "cl", "cnt")
        res = (
            res.withColumn("min", func.lit(None))
            .withColumn("max", func.lit(None))
            .withColumn("feature", func.lit(col))
        )
    elif isDate:
        temp_df = temp_df.withColumn(col, func.unix_timestamp(col))
        w = Window.orderBy(col)
        temp_df = temp_df.withColumn(
            "quantile_group", func.floor(func.percent_rank().over(w) * ptile) + 1
        )
        temp_df = temp_df.withColumn(
            "quantile_group",
            func.when(temp_df.quantile_group == ptile + 1, ptile).otherwise(
                temp_df.quantile_group
            ),
        )
        res = temp_df.groupBy("quantile_group").agg(
            func.min(col).alias("min"),
            func.max(col).alias("max"),
            func.sum("avg_bal").alias("bal"),
            func.sum("creditLimit").alias("cl"),
            func.count("avg_bal").alias("cnt"),
        )
        res = res.withColumn("feature", func.lit(col))
    elif isBoolean:
        temp_df = temp_df.withColumn(col, func.when(temp_df[col], 1).otherwise(0))
        w = Window.orderBy(col)
        temp_df = temp_df.withColumn(
            "quantile_group", func.floor(func.percent_rank().over(w) * ptile) + 1
        )
        temp_df = temp_df.withColumn(
            "quantile_group",
            func.when(temp_df.quantile_group == ptile + 1, ptile).otherwise(
                temp_df.quantile_group
            ),
        )
        res = temp_df.groupBy("quantile_group").agg(
            func.min(col).alias("min"),
            func.max(col).alias("max"),
            func.sum("avg_bal").alias("bal"),
            func.sum("creditLimit").alias("cl"),
            func.count("avg_bal").alias("cnt"),
        )
        res = res.withColumn("feature", func.lit(col))
    else:
        w = Window.orderBy(col)
        temp_df = temp_df.withColumn(
            "quantile_group", func.floor(func.percent_rank().over(w) * ptile) + 1
        )
        temp_df = temp_df.withColumn(
            "quantile_group",
            func.when(temp_df.quantile_group == ptile + 1, ptile).otherwise(
                temp_df.quantile_group
            ),
        )
        res = temp_df.groupBy("quantile_group").agg(
            func.min(col).alias("min"),
            func.max(col).alias("max"),
            func.sum("avg_bal").alias("bal"),
            func.sum("creditLimit").alias("cl"),
            func.count("avg_bal").alias("cnt"),
        )
        res = res.withColumn("feature", func.lit(col))
    res = res.withColumn("util", res.bal / res.cl)
    res = res.withColumn("isString", func.lit(isString))
    return res

spark = SparkSession.builder.getOrCreate()

pyspark_df = spark.sql(
    """
    SELECT
      *
    FROM
      neo_views_credit_risk.wk_utilization_v1_data
    WHERE
      isCreditActive = True
  """
)

schema = StructType(
    [
        StructField("feature", StringType()),
        StructField("quantile_group", StringType()),
        StructField("min", DecimalType()),
        StructField("max", DecimalType()),
        StructField("bal", DecimalType()),
        StructField("cl", DecimalType()),
        StructField("cnt", LongType()),
        StructField("util", FloatType()),
        StructField("isString", BooleanType()),
    ]
)

final_out = spark.createDataFrame([], schema)

for k, v in pyspark_df.dtypes:
    if k != "accountId":
        if v == "string":
            union_me = quantile_plot(pyspark_df, k, isString=True)
        elif v == "date":
            union_me = quantile_plot(pyspark_df, k, isDate=True)
        elif v == "boolean":
            union_me = quantile_plot(pyspark_df, k, isBoolean=True)
        else:
            union_me = quantile_plot(pyspark_df, k)
        final_out = final_out.unionByName(union_me)

final_out.toPandas().to_csv(
    "/Workspace/Users/wilson.kan@neofinancial.com/ValuationV1/for_eda_no_inactive.csv"
)

# COMMAND ----------


