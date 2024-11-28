# Databricks notebook source
feature = "currentCreditScore"
time_init = '2022-10-31'
time_comp = '2024-10-31'
num_seg = 20

# COMMAND ----------

query_init = f"""
SELECT {feature}
FROM neo_trusted_analytics.earl_account
WHERE referenceDate = '{time_init}'
and {feature} is not null
"""
data_init = spark.sql(query_init)

query_comp = f"""
SELECT {feature}
FROM neo_trusted_analytics.earl_account
WHERE referenceDate = '{time_comp}'
and {feature} is not null
"""
data_comp = spark.sql(query_comp)

# COMMAND ----------

df_init = data_init.toPandas()
df_comp = data_comp.toPandas()

# COMMAND ----------

import pandas as pd
[cuts_init, bins] = pd.qcut(df_init[feature], q=num_seg, retbins=True, labels=False)
cuts_comp = pd.cut(df_comp[feature], bins=bins, labels=False)
labels = [str(a) for a in pd.qcut(df_init[feature], q=num_seg).cat.categories]

# COMMAND ----------

ci = (cuts_init.groupby(cuts_init).count() / cuts_init.count()).to_frame()
ci.columns = ['init']

# COMMAND ----------

cc = (cuts_comp.groupby(cuts_comp).count() / cuts_comp.count()).to_frame()
cc.columns = ['comp']

# COMMAND ----------

res = ci.join(cc)
res.index = labels

# COMMAND ----------

import numpy as np
res['sum_this'] = (res['init'] - res['comp']) * np.log(res['init']/res['comp'])

# COMMAND ----------

print("PSI: ", res['sum_this'].sum(), "\n")

res

# COMMAND ----------

bins

# COMMAND ----------

# < 0.1 is good
# < 0.2 is ok
# changed
