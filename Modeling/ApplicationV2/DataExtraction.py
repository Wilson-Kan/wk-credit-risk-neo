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
# ea_app = spark.sql("""SELECT * FROM neo_trusted_analytics.earl_application""")
hard_cc = spark.sql(
    """SELECT _id as hardCreditReportId, userId, explode(details.trades.balance) FROM neo_raw_production.identity_service_transunion_hard_credit_check_reports"""
)
# soft_cc = spark.sql("""SELECT * FROM neo_raw_production.identity_service_transunion_soft_credit_check_reports""")
# link_info = spark.sql("""SELECT * FROM neo_raw_production.identity_service_user_reports_metadata_snapshots""")
# house = spark.sql("""SELECT * FROM neo_raw_production.user_service_users""")
# on_app = spark.sql("""SELECT * FROM neo_raw_production.credit_onboarding_service_credit_applications""")

# COMMAND ----------

display(hard_cc)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

db = "neo_raw_production"
spark = SparkSession.builder.getOrCreate()

hard_checks = spark.read.table(
    f"{db}.identity_service_transunion_hard_credit_check_reports"
).selectExpr(
    "_id as hardCreditReportId",
    "userId",
    "explode(details) as trades",
    "createdAt",
    "*",
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

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
tucrcs_ds = spark.sql(
  """
    with
      -- PART 1. COLLECT SOFT CHECK REPORTS
      vars_old AS (
        SELECT
          _id AS softCheckReportId
          ,details.accountNetCharacteristics AS tu_vars
        FROM neo_raw_production.application_service_transunion_soft_credit_reports -- PRE-JITO
      ),

      vars_new AS (
        SELECT
          _id AS softCheckReportId
          ,details.accountNetCharacteristics AS tu_vars
        FROM neo_raw_production.identity_service_transunion_soft_credit_check_reports -- AFTER JITO
      ), --- warning: there are still some soft check reports that are missing

      -- PART 2. COMBINE CRCS TOGETHER
      vars_all AS (
        SELECT 
          softCheckReportId
          ,tu_vars 
        FROM (
          SELECT * FROM vars_old UNION 
          SELECT * FROM vars_new
        )
      ),
        
      vars_raw AS (
        SELECT 
          softCheckReportId
          ,MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(tu.id, tu.value))) params
        FROM vars_all LATERAL VIEW INLINE(tu_vars) tu
        GROUP BY softCheckReportId
      ),
      
      -- PART 3. FEATURE EXTRACTION & FORMATTING
      df_vars AS (
        SELECT
          softCheckReportId
          ,EXPLODE(params)
        FROM vars_raw
      ),

      df_vars_crc AS (
        SELECT * FROM df_vars 
        PIVOT (
          SUM(CAST(value AS INT)) AS crcValue FOR key IN (
          "AM02"
          ,"AM04"
          ,"AM07"
          ,"AM167"
          ,"AM21"
          ,"AM216"
          ,"AM29"
          ,"AM33"
          ,"AM34"
          ,"AM36"
          ,"AM41"
          ,"AM42"
          ,"AM43"
          ,"AM44"
          ,"AM57"
          ,"AM60"
          ,"AM84"
          ,"AM91"
          ,"AT01"
          ,"AT02"
          ,"AT07"
          ,"AT21"
          ,"AT29"
          ,"AT33"
          ,"AT34"
          ,"AT36"
          ,"AT60"
          ,"AT84"
          ,"BC02"
          ,"BC141"
          ,"BC142"
          ,"BC143"
          ,"BC144"
          ,"BC145"
          ,"BC147"
          ,"BC148"
          ,"BC21"
          ,"BC33"
          ,"BC34"
          ,"BC36"
          ,"BC60"
          ,"BC62"
          ,"BC75"
          ,"BC76"
          ,"BC77"
          ,"BC78"
          ,"BC79"
          ,"BC80"
          ,"BC84"
          ,"BC85"
          ,"BC86"
          ,"BC91"
          ,"BC94"
          ,"BR02"
          ,"BR60"
          ,"BR62"
          ,"BR84"
          ,"BR91"
          ,"GO06"
          ,"GO07"
          ,"GO11"
          ,"GO14"
          ,"GO141"
          ,"GO148"
          ,"GO149"
          ,"GO15"
          ,"GO151"
          ,"GO152"
          ,"GO17"
          ,"GO21"
          ,"GO80"
          ,"GO81"
          ,"GO83"
          -- ,"GO91"
          ,"IN60"
          ,"IN84"
          ,"MC60"
          ,"PR09"
          ,"PR10"
          ,"PR100"
          ,"PR11"
          ,"PR116"
          ,"PR117"
          ,"PR119"
          ,"PR120"
          ,"PR123"
          ,"PR124"
          ,"PR14"
          ,"PR15"
          ,"PR21"
          ,"PR22"
          ,"PR30"
          ,"PR41"
          ,"PR42"
          ,"PR43"
          ,"PR44"
          ,"PR45"
          ,"PR46"
          ,"PR47"
          ,"PR50"
          ,"PR51"
          ,"PR52"
          ,"PR68"
          ,"PR69"
          ,"PR70"
          ,"PR73"
          ,"PR74"
          ,"PR75"
          ,"PR95"
          ,"PR97"
          ,"PR98"
          ,"RE01"
          ,"RE02"
          ,"RE03"
          ,"RE05"
          ,"RE06"
          ,"RE07"
          ,"RE09"
          ,"RE28"
          ,"RE29"
          ,"RE33"
          ,"RE336"
          ,"RE34"
          ,"RE35"
          ,"RE37"
          ,"RE38"
          ,"RE41"
          ,"RE42"
          ,"RE43"
          ,"RE60"
          ,"RE61"
          ,"RE62"
          ,"RE75"
          ,"RE76"
          ,"RE77"
          ,"RE81"
          ,"RE82"
          ,"RE83"
          ,"RE84"
          ,"RE91"
          ,"RR02"
          ,"RR60"
          ,"RR62"
          ,"RR84"
          ,"RR91"
          ,"SD60"
          ,"SL60")
        )
      ),

      df_vars_death AS (
        SELECT 
          softCheckReportId
          ,CASE WHEN params.GO91 = 'Y' THEN 1 WHEN params.GO91 = 'N' THEN 0 END AS tuDeceasedFlagGO91 --GO91 
        FROM vars_raw
      ),

      df_vars_final AS (
        SELECT * 
        FROM df_vars_death a FULL JOIN df_vars_crc b USING (softCheckReportId)
      )

    select * from df_vars_final
  """
)

# COMMAND ----------

display(tucrcs_ds)

# COMMAND ----------


tu_hard_crcs_ds = spark.sql(
  """
    with
      -- PART 1. COLLECT SOFT CHECK REPORTS
      vars_old AS (
        SELECT
          _id AS hardcheckReportId
          ,details.trades AS tu_vars
        FROM neo_raw_production.application_service_transunion_hard_credit_reports -- PRE-JITO
      ),

      vars_new AS (
        SELECT
          _id AS hardcheckReportId
          ,details.trades AS tu_vars
        FROM neo_raw_production.identity_service_transunion_hard_credit_check_reports -- AFTER JITO
      ), --- warning: there are still some soft check reports that are missing

      -- PART 2. COMBINE CRCS TOGETHER
      vars_all AS (
        SELECT 
          hardcheckReportId
          ,tu_vars 
        FROM (
          SELECT * FROM vars_old UNION 
          SELECT * FROM vars_new
        )
      ),
        
      vars_raw AS (
        SELECT 
          hardcheckReportId
          ,MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(tu.id, tu.value))) params
        FROM vars_all LATERAL VIEW INLINE(tu_vars) tu
        GROUP BY softCheckReportId
      ),
      
      -- PART 3. FEATURE EXTRACTION & FORMATTING
      df_vars AS (
        SELECT
          hardcheckReportId
          ,EXPLODE(params)
        FROM vars_raw
      )
      -- ,

      -- df_vars_crc AS (
      --   SELECT * FROM df_vars 
      --   PIVOT (
      --     SUM(CAST(value AS INT)) AS crcValue FOR key IN (
      --     "AM02"
      --     ,"AM04"
      --     ,"AM07"
      --     ,"AM167"
      --     ,"AM21"
      --     ,"AM216"
      --     ,"AM29"
      --     ,"AM33"
      --     ,"AM34"
      --     ,"AM36"
      --     ,"AM41"
      --     ,"AM42"
      --     ,"AM43"
      --     ,"AM44"
      --     ,"AM57"
      --     ,"AM60"
      --     ,"AM84"
      --     ,"AM91"
      --     ,"AT01"
      --     ,"AT02"
      --     ,"AT07"
      --     ,"AT21"
      --     ,"AT29"
      --     ,"AT33"
      --     ,"AT34"
      --     ,"AT36"
      --     ,"AT60"
      --     ,"AT84"
      --     ,"BC02"
      --     ,"BC141"
      --     ,"BC142"
      --     ,"BC143"
      --     ,"BC144"
      --     ,"BC145"
      --     ,"BC147"
      --     ,"BC148"
      --     ,"BC21"
      --     ,"BC33"
      --     ,"BC34"
      --     ,"BC36"
      --     ,"BC60"
      --     ,"BC62"
      --     ,"BC75"
      --     ,"BC76"
      --     ,"BC77"
      --     ,"BC78"
      --     ,"BC79"
      --     ,"BC80"
      --     ,"BC84"
      --     ,"BC85"
      --     ,"BC86"
      --     ,"BC91"
      --     ,"BC94"
      --     ,"BR02"
      --     ,"BR60"
      --     ,"BR62"
      --     ,"BR84"
      --     ,"BR91"
      --     ,"GO06"
      --     ,"GO07"
      --     ,"GO11"
      --     ,"GO14"
      --     ,"GO141"
      --     ,"GO148"
      --     ,"GO149"
      --     ,"GO15"
      --     ,"GO151"
      --     ,"GO152"
      --     ,"GO17"
      --     ,"GO21"
      --     ,"GO80"
      --     ,"GO81"
      --     ,"GO83"
      --     -- ,"GO91"
      --     ,"IN60"
      --     ,"IN84"
      --     ,"MC60"
      --     ,"PR09"
      --     ,"PR10"
      --     ,"PR100"
      --     ,"PR11"
      --     ,"PR116"
      --     ,"PR117"
      --     ,"PR119"
      --     ,"PR120"
      --     ,"PR123"
      --     ,"PR124"
      --     ,"PR14"
      --     ,"PR15"
      --     ,"PR21"
      --     ,"PR22"
      --     ,"PR30"
      --     ,"PR41"
      --     ,"PR42"
      --     ,"PR43"
      --     ,"PR44"
      --     ,"PR45"
      --     ,"PR46"
      --     ,"PR47"
      --     ,"PR50"
      --     ,"PR51"
      --     ,"PR52"
      --     ,"PR68"
      --     ,"PR69"
      --     ,"PR70"
      --     ,"PR73"
      --     ,"PR74"
      --     ,"PR75"
      --     ,"PR95"
      --     ,"PR97"
      --     ,"PR98"
      --     ,"RE01"
      --     ,"RE02"
      --     ,"RE03"
      --     ,"RE05"
      --     ,"RE06"
      --     ,"RE07"
      --     ,"RE09"
      --     ,"RE28"
      --     ,"RE29"
      --     ,"RE33"
      --     ,"RE336"
      --     ,"RE34"
      --     ,"RE35"
      --     ,"RE37"
      --     ,"RE38"
      --     ,"RE41"
      --     ,"RE42"
      --     ,"RE43"
      --     ,"RE60"
      --     ,"RE61"
      --     ,"RE62"
      --     ,"RE75"
      --     ,"RE76"
      --     ,"RE77"
      --     ,"RE81"
      --     ,"RE82"
      --     ,"RE83"
      --     ,"RE84"
      --     ,"RE91"
      --     ,"RR02"
      --     ,"RR60"
      --     ,"RR62"
      --     ,"RR84"
      --     ,"RR91"
      --     ,"SD60"
      --     ,"SL60")
      --   )
      -- ),

      -- df_vars_death AS (
      --   SELECT 
      --     softCheckReportId
      --     ,CASE WHEN params.GO91 = 'Y' THEN 1 WHEN params.GO91 = 'N' THEN 0 END AS tuDeceasedFlagGO91 --GO91 
      --   FROM vars_raw
      -- ),

      -- df_vars_final AS (
      --   SELECT * 
      --   FROM df_vars_death a FULL JOIN df_vars_crc b USING (softCheckReportId)
      -- )

    -- select * from df_vars_final
    select * from df_vars
  """
)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
test = spark.sql(
    """
select
  *
from 
  neo_raw_production.credit_onboarding_service_credit_applications as ea
  inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms
  inner join neo_raw_production.identity_service_transunion_soft_credit_check_reports as sc
  inner join neo_raw_production.identity_service_transunion_hard_credit_check_reports as hc
  on ea.userReportsMetadataSnapshotId = ms._id
  and ms.transunionSoftCreditCheckResult.reportId = sc._id
  and ms.transunionHardCreditCheckResult.reportId = hc._id
"""
)

# COMMAND ----------

test.count()

# COMMAND ----------


