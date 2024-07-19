# Databricks notebook source
# MAGIC %sql
# MAGIC select
# MAGIC   accountId,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when paymentVolume < 0 then 1
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as payment_count,
# MAGIC   -1 * max(grossCreditChargeOffExpense) as loss
# MAGIC from
# MAGIC   neo_trusted_analytics.earl_account
# MAGIC where
# MAGIC   isAccountChargedOff = true
# MAGIC group by
# MAGIC   accountId

# COMMAND ----------

payment_cnt = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   beh.accountId,
# MAGIC   beh.bad_rate,
# MAGIC   beh.modelScore,
# MAGIC   beh.risk_flag
# MAGIC from
# MAGIC   neo_data_science_production.credit_risk_behavior_pd_v2_1 as beh
# MAGIC where
# MAGIC   beh.referenceDate = '2024-01-31'

# COMMAND ----------

beh = _sqldf

# COMMAND ----------

from pyspark.sql import functions as F
res = beh.join(payment_cnt, beh.accountId == payment_cnt.accountId, "inner")
res = res.withColumn('payment_count_floored', F.least(res.payment_count, F.lit(3))) 

# COMMAND ----------

output = res.groupBy(['risk_flag', 'payment_count_floored']).sum('loss')

# COMMAND ----------

display(output)

# COMMAND ----------

output_2 = res.groupBy(['risk_flag', 'payment_count_floored']).count()

# COMMAND ----------

display(output_2)

# COMMAND ----------


