# Databricks notebook source
# import pyspark.sql.functions as F
# from databricks.sdk.runtime import *
# from pyspark.sql.session import SparkSession


# def main(spark: SparkSession, db: str = "neo_raw_production"):
#     """Get application data for monthly BKO scoring"""
#     hard_checks = (
#         spark.table(f"{db}.identity_service_transunion_hard_credit_check_reports")
#         .selectExpr(
#             "_id as hardCreditReportId", "userId", "explode(details.trades) as trades", "createdAt"
#         )
#         .filter(F.col("trades.type") == "I")
#         .selectExpr(
#             "hardCreditReportId",
#             "cast(trades.balance as int) as balance",
#             "if(trades.creditLimit = 0, trades.highCredit, trades.creditLimit) as finalLimit",
#             """make_date(
#                 left(trades.dateLastActivity, 4),
#                 substring(trades.dateLastActivity, 5, 2),
#                 right(trades.dateLastActivity, 2)
#             ) as dateLastActivity""",
#             "datediff(month, dateLastActivity, createdAt) as monthsSinceLastActivity",
#             "createdAt",
#         )
#         .groupBy("hardCreditReportId")
#         .agg(
#             F.expr(
#                 "coalesce(avg(if(monthsSinceLastActivity <= 12 and balance>0, finalLimit, null)),-1) as avgLimitActiveInstallmentsPast12Months"
#             ),
#             F.expr(
#                 "sum(if(monthsSinceLastActivity <= 12 and balance>0, 1, 0)) as numActiveInstallmentsPast12Months"
#             ),
#         )
#     )

#     soft_checks = spark.table(
#         f"{db}.identity_service_transunion_soft_credit_check_reports"
#     ).selectExpr(
#         "_id as softCheckReportId",
#         "map_from_entries(details.accountNetCharacteristics) as accountNetCharacteristics",
#         "cast(accountNetCharacteristics.AT07 as integer) as AT07",
#         "cast(accountNetCharacteristics.AT60 as integer) as AT60",
#         "cast(accountNetCharacteristics.BC145 as integer) as BC145",
#         "cast(accountNetCharacteristics.GO06 as integer) as GO06",
#         "cast(accountNetCharacteristics.RE38 as integer) as RE38",
#         "cast(details.scoreProduct.score as integer) as CVSC100",
#     )

#     snapshots = spark.table(f"{db}.identity_service_user_reports_metadata_snapshots").selectExpr(
#         "_id as userReportsMetadataSnapshotId",
#         "transunionHardCreditCheckResult.reportId as hardCreditReportId",
#         "transunionSoftCreditCheckResult.reportId as softCheckReportId",
#     )

#     applications = (
#         spark.table(f"{db}.credit_onboarding_service_credit_applications")
#         .selectExpr(
#             "_id as applicationId",
#             "userId",
#             "userReportsMetadataSnapshotId",
#             "brand",
#             """case when origin in ('IN_STORE', 'INSTORE') then 'INSTORE' when origin = 'ONLINE' then 'ONLINE' else 'OTHERS' end as origin""",
#             "concat_ws(' ', decision, status) as decisionRawData",
#             "FROM_UTC_TIMESTAMP(createdAt, 'America/Edmonton') as applicationDate",
#         )
#         .filter(F.col("decisionRawData") == "APPROVED COMPLETED")
#     )

#     housing_status = spark.table(f"{db}.user_service_users").selectExpr(
#         "_id as userId", "housingStatus"
#     )

#     return (
#         applications.join(snapshots, "userReportsMetadataSnapshotId", "left")
#         .join(soft_checks, "softCheckReportId", "left")
#         .join(hard_checks, "hardCreditReportId", "left")
#         .join(housing_status, "userId", "left")
#         .select(
#             "applicationId",
#             "userId",
#             "AT07",
#             "AT60",
#             "avgLimitActiveInstallmentsPast12Months",
#             "BC145",
#             "brand",
#             "CVSC100",
#             "GO06",
#             "housingStatus",
#             "numActiveInstallmentsPast12Months",
#             "origin",
#             "RE38",
#             "applicationDate",
#         )
#     )

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
ea_app = spark.sql("""SELECT * FROM neo_trusted_analytics.earl_application""")
hard_cc = spark.sql("""SELECT * FROM neo_raw_production.identity_service_transunion_hard_credit_check_reports""")
soft_cc = spark.sql("""SELECT * FROM neo_raw_production.identity_service_transunion_soft_credit_check_reports""")
link_info = spark.sql("""SELECT * FROM neo_raw_production.identity_service_user_reports_metadata_snapshots""")
house = spark.sql("""SELECT * FROM neo_raw_production.user_service_users""")
on_app = spark.sql("""SELECT * FROM neo_raw_production.credit_onboarding_service_credit_applications""") 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

db = "neo_raw_production"
spark = SparkSession.builder.getOrCreate()

hard_checks = (
    spark.read.table(f"{db}.identity_service_transunion_hard_credit_check_reports")
    .selectExpr(
        "_id as hardCreditReportId",
        "userId",
        "explode(details) as trades",
        "createdAt",
        "*"
    )
)

# COMMAND ----------

display(hard_checks)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

db = "neo_raw_production"
spark = SparkSession.builder.getOrCreate()

hard_checks = (
    spark.read.table(f"{db}.identity_service_transunion_hard_credit_check_reports")
    .selectExpr(
        "_id as hardCreditReportId",
        "userId",
        "explode(details.trades) as trades",
        "createdAt",
    )
    .filter(F.col("trades.type") == "I")
    .selectExpr(
        "hardCreditReportId",
        "cast(trades.balance as int) as balance",
        "if(trades.creditLimit = 0, trades.highCredit, trades.creditLimit) as finalLimit",
        """make_date(
                left(trades.dateLastActivity, 4),
                substring(trades.dateLastActivity, 5, 2),
                right(trades.dateLastActivity, 2)
            ) as dateLastActivity""",
        "datediff(month, dateLastActivity, createdAt) as monthsSinceLastActivity",
        "createdAt",
    )
    .groupBy("hardCreditReportId")
    .agg(
        F.expr(
            "coalesce(avg(if(monthsSinceLastActivity <= 12 and balance>0, finalLimit, null)),-1) as avgLimitActiveInstallmentsPast12Months"
        ),
        F.expr(
            "sum(if(monthsSinceLastActivity <= 12 and balance>0, 1, 0)) as numActiveInstallmentsPast12Months"
        ),
    )
)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
abc = spark.sql("""SELECT * FROM neo_trusted_analytics.earl_application""")

# COMMAND ----------

abc.columns

# COMMAND ----------


