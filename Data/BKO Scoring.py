# Databricks notebook source
# load libraries
# import math
# import numpy as np
# import pandas as pd
# from datetime import date
# import matplotlib.pyplot as plt
# from sklearn.metrics import roc_auc_score

# import pyspark.sql.types as T
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window

# COMMAND ----------

# # function used to convert scores to probability values
# def probability_calc(df_spark, calibration_diff: int = 0):
#     """
#     A function that calculate probabilities by converting scores into probability values as a new column.
#     """
#     points0 = 700
#     odds0 = 50
#     pdo = 30
#     log2 = 0.6931471805599453
#     factor = pdo / log2
#     offset = points0 - (factor * math.log(odds0))

#     df_with_p = df_spark.withColumn(
#         "odds", F.exp((F.lit(offset) - (F.col("score") + calibration_diff)) / F.lit(factor))
#     ).withColumn("probability", F.round(F.col("odds") / (1 + F.col("odds")), 6))

#     return df_with_p

# # create datatable for monitoring
# user_info_raw = spark.sql(
#     """
#     with
#     -- select all accounts for unsecured credit card products (population for bko model)
#     accts as (
#         select
#             accountId as creditAccountId
#             ,openedAt_mt as accountOpenDate
#             ,userId
#         from neo_trusted_analytics.earl_account
#         where subProductName = "UNSECURED"
#     ),

#     -- label all accounts closed due to bankruptcy as 1 (wentBankrupt)
#     labels as (
#         select 
#             creditAccountId
#             ,to_date(chargeOffDate_mt) as chargeOffDate
#             ,1 as wentBankrupt
#         from neo_trusted_analytics.charge_offs
#         where chargeOffReason in ("CREDIT_COUNSELLING_SOLUTIONS", "CONSUMER_PROPOSALS", "BANKRUPTCY")
#     ),

#     -- user level table with labels
#     users as (
#         select 
#             a.userId
#             ,case when max(b.wentBankrupt) is null then 0 else 1 end as wentBankrupt
#             ,min(b.chargeOffDate) as chargeOffDate -- choose the earliest bankruptcy date for each user
#             ,min(a.accountOpenDate) as accountOpenDate -- choose the earliest account open date for each user
#         from accts a left join labels b on a.creditAccountId = b.creditAccountId
#         group by a.userId
#     )

#     select * from users
#     """
# )

# # backfill all missing months
# today = date.today().strftime("%Y-%m-%d")
# all_months = pd.DataFrame(pd.date_range('2024-01-01', today, freq='MS')).rename(columns={0:"monthRaw"})
# month_info = spark.createDataFrame(all_months)
# user_info = (
#   user_info_raw.crossJoin(month_info.select("monthRaw"))
#   .filter(F.col("monthRaw")>F.col("accountOpenDate"))
#   .withColumn("month", F.date_format(F.col("monthRaw"), "yyyy-MM-dd"))
#   .select("userId", "month", "wentBankrupt", "chargeOffDate")
# )

# tu_features = (
#     # collect features from TransUnion credit monthly reports
#     spark.table('neo_raw_production.transunion_creditreport_creditvision')
#     .select(
#         'user_id', 'credit_report_date', 
#         'AM07', 'AM33', 'AM167', 'AT60', 'BC60', 'GO21', 'CVSC100', 
#         'GO14', 'AT29', 'AM02'
#     )
#     .withColumn('creditReportDate', F.to_date(F.col("credit_report_date").cast("string"), "yyyyMMdd"))
#     .withColumn('month', F.month(F.col('creditReportDate')))
#     .withColumn('year', F.year(F.col('creditReportDate')))

#     # keep the latest one if there are multiple credit reports for same user in a certain month
#     .withColumn('reportOrder', F.row_number().over(Window.partitionBy("user_id", "month", "year").orderBy("creditReportDate")))
#     .withColumn('month_end_date', F.last_day(F.col('creditReportDate')))
#     .withColumn('max_row', F.max('reportOrder').over(Window.partitionBy('user_id', 'month_end_date').orderBy(F.col("month_end_date"))))
#     .filter(F.col('max_row')==F.col('reportOrder'))
#     .drop('reportOrder', 'max_row', 'month', 'year', 'credit_report_date', 'month_end_date')

#     # data cleaning
#     .withColumnRenamed('user_id', 'userId')
#     .withColumn("AM07", F.col("AM07").cast(T.IntegerType()))
#     .withColumn("AM33", F.col("AM33").cast(T.IntegerType()))
#     .withColumn("AM167", F.col("AM167").cast(T.IntegerType()))
#     .withColumn("AT60", F.col("AT60").cast(T.IntegerType()))
#     .withColumn("BC60", F.col("BC60").cast(T.IntegerType()))
#     .withColumn("GO21", F.col("GO21").cast(T.IntegerType()))
#     .withColumn("GO14", F.col("GO14").cast(T.IntegerType()))
#     .withColumn("AT29", F.col("AT29").cast(T.IntegerType()))
#     .withColumn("AM02", F.col("AM02").cast(T.IntegerType()))
#     .withColumn("month", F.date_format(F.col("creditReportDate"), "yyyy-MM-dd"))
# )

# other_features= spark.sql(
#     """
#     with
#     -- those features are not user level, but application level, only get them once during application 
#     -- they are troublemakers since not included in EARL table yet, so have to query from the upstream raw tables
#     approved_appls as (
#         select
#             _id as applicationId
#             ,to_date(createdAt) as applicationDate
#             ,userId
#             ,'NEO' as brand
#             ,case when steps.housingstatus.data.housingstatus is null then 'UNKNOWN' else UPPER(steps.housingstatus.data.housingstatus) end as housing
#             ,case when steps.adjudication.input.applicationvalues.dti is null then -1 else ROUND(steps.adjudication.input.applicationvalues.dti, 2) end as DTI
#         from neo_raw_production.application_service_credit_account_applications 
#         where decision = "APPROVED" and status = "COMPLETED"
#         union
#         select
#             _id as applicationId
#             ,to_date(createdAt) as applicationDate
#             ,userId
#             ,'HBC' as brand
#             ,case when steps.housingstatus.data.housingstatus is null then 'UNKNOWN' else UPPER(steps.housingstatus.data.housingstatus) end as housing
#             ,case when steps.adjudication.input.applicationvalues.dti is null then -1 else ROUND(steps.adjudication.input.applicationvalues.dti, 2) end as DTI
#         from neo_raw_production.application_service_hbc_credit_applications 
#         where decision = "APPROVED" and status = "COMPLETED"
#         union
#         select
#             _id as applicationId
#             ,to_date(createdAt) as applicationDate
#             ,userId
#             ,'HBC' as brand
#             ,case when steps.housingstatus.data.housingstatus is null then 'UNKNOWN' else UPPER(steps.housingstatus.data.housingstatus) end as housing
#             ,case when steps.adjudication.input.applicationvalues.dti is null then -1 else ROUND(steps.adjudication.input.applicationvalues.dti, 2) end as DTI
#         from neo_raw_production.application_service_hbc_instore_credit_applications 
#         where decision = "APPROVED" and status = "COMPLETED"
#         union
#         select
#             a._id as applicationId
#             ,to_date(a.createdAt) as applicationDate
#             ,a.userId
#             ,a.brand
#             ,case when b.housing.status is null then 'UNKNOWN' else upper(b.housing.status) end as housing
#             ,case when a.creditInformation.debtToIncome is null then -1 else round(a.creditInformation.debtToIncome, 2) end as DTI
#         from neo_raw_production.credit_onboarding_service_credit_applications a
#         left join neo_raw_production.unified_onboarding_service_application_metadata b on a.applicationMetadataId = b._id
#         where a.decision = "APPROVED" and a.status = "COMPLETED" and a.type = "STANDARD"
#     ),

#     mapping_appls_to_accts as (
#         select a.*, b._id as creditAccountId from approved_appls a
#         inner join neo_raw_production.bank_service_credit_accounts b on a.applicationId = b.creditApplicationId
#     ),

#     -- when Adriaan made this scorecard, one user can only had one credit account
#     -- for brand, housing status, debtToIncome (DTI), let's take them from the applications associated to their first credit account
#     -- otherwise the bko score generated might keep changing if using other methods (e.g. use application associated to their latest credit account)
#     ordered_records as (
#         select *, row_number() over(partition by creditAccountId order by applicationDate asc) as applicationOrder from mapping_appls_to_accts
#     )

#     select userId, brand, housing, DTI from ordered_records where applicationOrder = 1
#     """
# )

# cols_list = ["brand_score", "housing_score", "DTI_score", "AM07_score", "AM167_score", "AT60_score", "BC60_score", "GO21_score", "AM33_score", "CVSC100_score"]
# score_components = '+'.join(cols_list)

# rawdata = (
#     # for users that filed bankruptcy, remove all credit reports pulled after that date since it's meaningless to score them after c/o
#     user_info
#     .join(tu_features, ["userId", "month"], "left")
#     .join(other_features, "userId", "left")
#     .filter(~((F.col("chargeOffDate").isNotNull()) & (F.col("creditReportDate")>=F.col("chargeOffDate"))))
# )

# # backfill missing values using most recent data
# crcs_list = ['AM07', 'AM33', 'AM167', 'AT60', 'BC60', 'GO21', 'CVSC100', 'GO14', 'AT29', 'AM02']
# for i in crcs_list:
#     rawdata = (
#         rawdata
#         .withColumn(
#             i,
#             F.when(F.col(i).isNull(),
#             F.last(i, ignorenulls=True).over(Window.partitionBy("userId").orderBy("month"))).otherwise(F.col(i))
#             )
#     )

# alldata = (
#     rawdata
#     # if a user filed bankruptcy within 12 months after a certain credit report, then label this record as 1, else 0 (bko model target)
#     .withColumn(
#         "label", 
#         F.when((F.col("chargeOffDate").isNotNull()) & (F.months_between(F.col("chargeOffDate"), F.col("creditReportDate"))<=12), 1)
#         .otherwise(0)
#     )
#     .withColumn(
#         "safeSegment", 
#         F.when((F.col("GO14")<470) & (F.col("CVSC100")<839) & (F.col("AT29")>0) & (F.col("AM02")>0) & (F.col("AM33")>500), 0)
#         .otherwise(1)
#     )
    
#     # binning and scoring, for each feature
#     .withColumn(
#         'AM07_score',
#         F.when(F.col('AM07')<=0, 89)
#         .when(F.col('AM07')==1, 79)
#         .when(F.col('AM07')==2, 72)
#         .when(F.col('AM07')==3, 67)
#         .when(F.col('AM07')==4, 59)
#         .when(F.col('AM07')>4, 55)
#         .otherwise(89)
#     )
#     .withColumn(
#         'AM167_score',
#         F.when(F.col('AM167')<=0, 93)
#         .when(F.col('AM167')==1, 84)
#         .when(F.col('AM167')==2, 78)
#         .when(F.col('AM167')==3, 73)
#         .when(F.col('AM167')==4, 68)
#         .when(F.col('AM167')==5, 65)
#         .when(F.col('AM167')>5, 62)
#         .otherwise(93)
#     )
#     .withColumn(
#         'AM33_score',
#         F.when(F.col('AM33')<=800, 100)
#         .when(F.col('AM33').between(801, 3000), 90)
#         .when(F.col('AM33').between(3001, 7000), 80)
#         .when(F.col('AM33').between(7001, 10000), 73)
#         .when(F.col('AM33').between(10001, 25000), 72)
#         .when(F.col('AM33').between(25001, 40000), 69)
#         .when(F.col('AM33').between(40001, 100000), 64)
#         .when(F.col('AM33').between(100001, 500000), 84)
#         .when(F.col('AM33')>500000, 111)
#         .otherwise(100)
#     )
#     .withColumn(
#         'AT60_score',
#         F.when(F.col('AT60')<=0, 91)
#         .when(F.col('AT60').between(1, 200), 88)
#         .when(F.col('AT60').between(201, 500), 77)
#         .when(F.col('AT60').between(501, 1000), 73)
#         .when(F.col('AT60')>1000, 67)
#         .otherwise(91)
#     )
#     .withColumn(
#         'BC60_score',
#         F.when(F.col('BC60')<=10, 94)
#         .when(F.col('BC60').between(11, 25), 92)
#         .when(F.col('BC60').between(26, 50), 86)
#         .when(F.col('BC60').between(51, 100), 76)
#         .when(F.col('BC60').between(101, 200), 67)
#         .when(F.col('BC60')>200, 64)
#         .otherwise(94)
#     )
#     .withColumn(
#         'CVSC100_score',
#         F.when(F.col('CVSC100')<=620, 25)
#         .when(F.col('CVSC100').between(621, 640), 33)
#         .when(F.col('CVSC100').between(641, 660), 37)
#         .when(F.col('CVSC100').between(661, 680), 51)
#         .when(F.col('CVSC100').between(681, 700), 59)
#         .when(F.col('CVSC100').between(701, 720), 68)
#         .when(F.col('CVSC100').between(721, 740), 91)
#         .when(F.col('CVSC100').between(741, 760), 99)
#         .when(F.col('CVSC100').between(761, 800), 130)
#         .when(F.col('CVSC100')>800, 138)
#         .otherwise(25)
#     )
#     .withColumn(
#         'GO21_score',
#         F.when(F.col('GO21')<=0, 90)
#         .when(F.col('GO21')==1, 81)
#         .when(F.col('GO21')==2, 73)
#         .when(F.col('GO21')==3, 67)
#         .when(F.col('GO21').between(4, 5), 62)
#         .when(F.col('GO21')>5, 52)
#         .otherwise(90)
#     )
#     .withColumn(
#         'DTI_score',
#         F.when(F.col('DTI')<=25, 83)
#         .when((F.col('DTI')>25) & (F.col('DTI')<=30), 79)
#         .when(F.col('DTI')>30, 75)
#         .otherwise(75)
#     )
#     .withColumn(
#         'brand_score',
#         F.when(F.col('brand')=="HBC", 88)
#         .when(F.col('brand')=="NEO", 55)
#         .when(F.col('brand')=="SIENNA", 55)
#         .when(F.col('brand')=="CATHAY", 88)
#         .otherwise(55)
#     )
#     .withColumn(
#         'housing_score',
#         F.when(F.col('housing')=="OWN", 108)
#         .when(F.col('housing')=="LIVING_WITH_FAMILY", 66)
#         .otherwise(60)
#     )

#     # calculate final bko score
#     .withColumn(
#         "score", 
#         F.when(F.col("safeSegment")==0, F.expr(score_components))
#         .otherwise(999)
#     )

#     # formatting and data cleaning
#     .withColumn("wentBankrupt", F.col("wentBankrupt").cast(T.BooleanType()))
#     .withColumn("safeSegment", F.col("safeSegment").cast(T.BooleanType()))
#     .select(
#         "userId" # unique id
#         ,"month" # year and month (str) when the monthly credit report was pulled
#         ,"brand" # predictor
#         ,"brand_score"
#         ,"housing" # predictor
#         ,"housing_score"
#         ,"DTI" # predictor
#         ,"DTI_score" 
#         ,"AM07" # predictor
#         ,"AM07_score"
#         ,"AM167" # predictor
#         ,"AM167_score"
#         ,"AT60" # predictor
#         ,"AT60_score"
#         ,"BC60" # predictor
#         ,"BC60_score"
#         ,"GO21" # predictor
#         ,"GO21_score"
#         ,"AM33" # predictor, segmentation
#         ,"AM33_score"
#         ,"CVSC100" # predictor, segmentation
#         ,"CVSC100_score"
#         ,"GO14" # segmentation
#         ,"AT29" # segmentation
#         ,"AM02" # segmentation
#         ,"safeSegment" # if this user will be given 999 (highest score, safest)
#         ,"creditReportDate" # date when the monthly credit report was pulled
#         ,"chargeOffDate" # date when user filed bankruptcy
#         ,"wentBankrupt" # true or false, if this user ever filed bankruptcy
#         ,"label" # model target, file bankruptcy within next 12 months
#         ,"score"
#     )
#     .orderBy(F.col('userId'), F.col('month'))
# )

# # add columns for odds and probability
# finaldata = probability_calc(alldata)

# display(finaldata)

# # save the table
# # finaldata.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('neo_views_credit_risk.beh_bko_score_test')

# COMMAND ----------

# import pickle
# p_out = finaldata.toPandas()[['userId','month','brand','score']] 
# pickle.dump(p_out , open('/Workspace/Users/wilson.kan@neofinancial.com/bko_2024_Data.pkl', 'wb'))

# COMMAND ----------

import pickle
p_out = pickle.load(open('/Workspace/Users/wilson.kan@neofinancial.com/bko_2024_Data.pkl', 'rb'))
p_out.head()

# COMMAND ----------

def risk_flag(df):
    
    if (df['score'] <= 603):
        return '1 - Highest risk'
    elif (df['score'] <= 653) and (df['brand'] == 'NEO'):
        return '2 - High risk'
    elif (df['score'] <= 653):
        return '3 - Medium high risk'
    elif (df['score'] <= 697):
        return '4 - Medium low risk'
    elif (df['score'] <= 758):
        return '5 - Low risk'
    else:
        return '6 - Lowest risk'
      
p_out['risk_flag'] = p_out.apply(risk_flag, axis = 1)
p_out.sort_values('risk_flag')


# COMMAND ----------

#write to delta lake
df_sp = spark.createDataFrame(p_out)
df_sp.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('neo_views_credit_risk.bko_2024_01_06_segmented')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT userId, month, brand, score FROM neo_views_credit_risk.beh_bko_score_test where month >= '2024-01'

# COMMAND ----------

def risk_flag(df):
    
    if (df['score'] <= 603):
        return '1 - Highest risk'
    elif (df['score'] <= 653) and (df['brand'] == 'NEO'):
        return '2 - High risk'
    elif (df['score'] <= 653):
        return '3 - Medium high risk'
    elif (df['score'] <= 697):
        return '4 - Medium low risk'
    elif (df['score'] <= 758):
        return '5 - Low risk'
    else:
        return '6 - Lowest risk'
      
p_out = _sqldf.toPandas()
p_out['risk_flag'] = p_out.apply(risk_flag, axis = 1)
p_out.sort_values('risk_flag')

#write to delta lake
df_sp = spark.createDataFrame(p_out)
df_sp.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('neo_views_credit_risk.bko_2024_01_06_segmented_v2')

# COMMAND ----------


