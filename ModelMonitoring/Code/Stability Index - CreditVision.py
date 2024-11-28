# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta

feature = "BC34"
time_init = '2022-10-31'
t0 = datetime.strptime(time_init, "%Y-%m-%d") - relativedelta(days=30)
t1 = datetime.strptime(time_init, "%Y-%m-%d")
time_comp = '2024-10-31'
c0 = datetime.strptime(time_comp, "%Y-%m-%d") - relativedelta(days=30)
c1 = datetime.strptime(time_comp, "%Y-%m-%d")
num_seg = 100

# COMMAND ----------

query_init = f"""
SELECT CAST({feature} as float) as {feature}
FROM neo_raw_production.transunion_creditreport_creditvision
WHERE createdat between '{t0}' and '{t1}'
and {feature} is not null
"""
data_init = spark.sql(query_init)

query_comp = f"""
SELECT CAST({feature} as float) as {feature}
FROM neo_raw_production.transunion_creditreport_creditvision
WHERE createdat between '{c0}' and '{c1}'
and {feature} is not null
"""
data_comp = spark.sql(query_comp)

# COMMAND ----------

df_init = data_init.toPandas()
df_comp = data_comp.toPandas()

# COMMAND ----------

import pandas as pd
[cuts_init, bins] = pd.qcut(df_init[feature], q=num_seg, retbins=True, labels=False, duplicates='drop')
cuts_comp = pd.cut(df_comp[feature], bins=bins, labels=False)
labels = [str(a) for a in pd.qcut(df_init[feature], q=num_seg, duplicates='drop').cat.categories]

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

# < 0.1 is good
# < 0.2 is ok
# changed

# COMMAND ----------

from scipy.stats import ks_2samp

#perform Kolmogorov-Smirnov test
ks_res = ks_2samp(df_init[feature], df_comp[feature])
print(ks_res.pvalue, ks_res.statistic)

# COMMAND ----------


ks_res = ks_2samp(df_init[feature], df_init[feature])
print(ks_res.pvalue, ks_res.statistic)

# COMMAND ----------


