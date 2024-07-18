# Databricks notebook source
# MAGIC %sql
# MAGIC (select applicationId, brand, coalesce(cumulativeCreditAccountRevenue, 0) as cumulativeCreditAccountRevenue, coalesce(netCreditChargeOffExpense, 0) as netCreditChargeOffExpense from hive_metastore.neo_trusted_analytics.earl_account order by applicationId, referenceDate)

# COMMAND ----------


