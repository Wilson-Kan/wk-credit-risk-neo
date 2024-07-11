# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT a.brand, a.applicationId, a.ever90DPD_12M, a.accountOpenDate, b.segment, b.bkoScoreV1, b.thickScoreV1, b.thinScoreV1, b.subprimeScoreV1 FROM `hive_metastore`.`neo_views_credit_risk`.`cc_all_v1000_dbt_application_performance_earl` as a
# MAGIC INNER JOIN neo_views_credit_risk.cc_all_v1000_dbt_application_scores as b
# MAGIC on a.applicationId = b.applicationId
