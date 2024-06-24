# Databricks notebook source
# ####################################################################
# Libraries
# ####################################################################
# sql, pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import  col, to_date, when, concat, year, month, lit, lpad, dayofyear, weekofyear, array, explode, lower, expr, greatest, desc, concat_ws, to_timestamp,avg,row_number, round, last_day, substring
import datetime
import pytz
from pyspark.sql.window import Window
import re
import pyspark.sql.functions
from pyspark.sql.functions import month
from pyspark.sql.functions import to_timestamp,date_format, unix_timestamp

# basics for python
import math
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyspark.pandas as ps

# stats
# from statsmodels.stats.outliers_influence import variance_inflation_factor
# from statsmodels.formula.api import ols

# sklearn
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.inspection import permutation_importance
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_classif
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import ConfusionMatrixDisplay, roc_auc_score, confusion_matrix, mean_squared_error
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
import scipy.stats as stats
from scipy.stats import chi2_contingency

# xgboost
import xgboost as xgb
from xgboost import XGBClassifier, plot_tree

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 1. Data Preparation

# COMMAND ----------

# Observation window Oct 2021 - Dec 2022 - EARL TABLE

earl_account = (
  spark.table('neo_trusted_analytics.earl_account')
)

month_end_report = (
  earl_account
  .filter(col('referenceDate')==col('endOfMonthDate_')) #keep only month end date information

  # Target Definition
  .withColumn('maxDPD', F.when(col('daysPastDue')>=0,col('daysPastDue')).otherwise(0))
  .withColumn('maxDPD1', F.lag(col('maxDPD'),-1).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD2', F.lag(col('maxDPD'),-2).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD3', F.lag(col('maxDPD'),-3).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD4', F.lag(col('maxDPD'),-4).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD5', F.lag(col('maxDPD'),-5).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD6', F.lag(col('maxDPD'),-6).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD7', F.lag(col('maxDPD'),-7).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD8', F.lag(col('maxDPD'),-8).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD9', F.lag(col('maxDPD'),-9).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD10', F.lag(col('maxDPD'),-10).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD11', F.lag(col('maxDPD'),-11).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('maxDPD12', F.lag(col('maxDPD'),-12).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  
  .withColumn('CO_Insolvency', 
              F.when((col('chargeOffCategory')=='CREDIT') & 
                     (col('chargedOffReason').isin('CREDIT_COUNSELLING_SOLUTIONS', 'CONSUMER_PROPOSALS', 'BANKRUPTCY')),1)
               .otherwise(0))
              
  .withColumn('CO_Ins1', F.lag(col('CO_Insolvency'),-1).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins2', F.lag(col('CO_Insolvency'),-2).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins3', F.lag(col('CO_Insolvency'),-3).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins4', F.lag(col('CO_Insolvency'),-4).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins5', F.lag(col('CO_Insolvency'),-5).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins6', F.lag(col('CO_Insolvency'),-6).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins7', F.lag(col('CO_Insolvency'),-7).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins8', F.lag(col('CO_Insolvency'),-8).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins9', F.lag(col('CO_Insolvency'),-9).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins10', F.lag(col('CO_Insolvency'),-10).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins11', F.lag(col('CO_Insolvency'),-11).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  .withColumn('CO_Ins12', F.lag(col('CO_Insolvency'),-12).over(Window.partitionBy(col('accountId')).orderBy(col('endOfMonthDate_'))))
  
  .withColumn('bad0', F.when((col('maxDPD')>29) & (col('maxDPD')<90),0.5).when(col('maxDPD')>=90,1).otherwise(0))
  .withColumn('bad1', F.when((col('maxDPD1')>29) & (col('maxDPD1')<90),0.5).when(col('maxDPD1')>=90,1).otherwise(0))
  .withColumn('bad2', F.when((col('maxDPD2')>29) & (col('maxDPD2')<90),0.5).when(col('maxDPD2')>=90,1).otherwise(0))
  .withColumn('bad3', F.when((col('maxDPD3')>29) & (col('maxDPD3')<90),0.5).when(col('maxDPD3')>=90,1).otherwise(0))
  .withColumn('bad4', F.when((col('maxDPD4')>29) & (col('maxDPD4')<90),0.5).when(col('maxDPD4')>=90,1).otherwise(0))
  .withColumn('bad5', F.when((col('maxDPD5')>29) & (col('maxDPD5')<90),0.5).when(col('maxDPD5')>=90,1).otherwise(0))
  .withColumn('bad6', F.when((col('maxDPD6')>29) & (col('maxDPD6')<90),0.5).when(col('maxDPD6')>=90,1).otherwise(0))
  .withColumn('bad7', F.when((col('maxDPD7')>29) & (col('maxDPD7')<90),0.5).when(col('maxDPD7')>=90,1).otherwise(0))
  .withColumn('bad8', F.when((col('maxDPD8')>29) & (col('maxDPD8')<90),0.5).when(col('maxDPD8')>=90,1).otherwise(0))
  .withColumn('bad9', F.when((col('maxDPD9')>29) & (col('maxDPD9')<90),0.5).when(col('maxDPD9')>=90,1).otherwise(0))
  .withColumn('bad10', F.when((col('maxDPD10')>29) & (col('maxDPD10')<90),0.5).when(col('maxDPD10')>=90,1).otherwise(0))
  .withColumn('bad11', F.when((col('maxDPD11')>29) & (col('maxDPD11')<90),0.5).when(col('maxDPD11')>=90,1).otherwise(0))
  .withColumn('bad12', F.when((col('maxDPD12')>29) & (col('maxDPD12')<90),0.5).when(col('maxDPD12')>=90,1).otherwise(0))

   # The definition of default for the behavioral scorecard is 90+ days past due (DPD) in the upcoming 12 months
  .withColumn('bad_default', 
              greatest(col('bad1'),col('bad2'),col('bad3'),col('bad4'),col('bad5'),col('bad6'),col('bad7'),col('bad8'),col('bad9'),col('bad10'),col('bad11'),col('bad12'), #delinquency
                       col('CO_Ins1'),col('CO_Ins2'),col('CO_Ins3'),col('CO_Ins4'),col('CO_Ins5'),col('CO_Ins6'),col('CO_Ins7'),col('CO_Ins8'),col('CO_Ins9'),col('CO_Ins10'),col('CO_Ins11'),col('CO_Ins12')) #insolvency
              )


  .withColumn('bad', F.when((col('bad_default')==1),1).otherwise(0)) #Bad. 90+ dpd or CO by insolvency
  .withColumn('indeterminate', F.when((col('bad_default')==0.5) | ~(col('bad')==1),1).otherwise(0)) # (0,90) dpd 

  .filter((col('endOfMonthDate_')>='2021-10-31') & (col('endOfMonthDate_')<='2023-05-31'))
)

# All UNSECURED accounts
month_end_report_v0 = (
  month_end_report
  .filter(col('productTypeName').isin('HBC UNSECURED STANDARD CREDIT','NEO UNSECURED STANDARD CREDIT','SIENNA UNSECURED STANDARD CREDIT','CATHAY UNSECURED WORLD ELITE CREDIT'))
)

# Exclusion of Fraud accounts
fraud_accounts = (
  month_end_report_v0
  .filter(col('chargeOffCategory')=='FRAUD')
)

month_end_report_v1 = (
  month_end_report_v0
  .join(fraud_accounts, ['accountId'], how='left_anti')
)

# Exclusion of Deceased and Other change-off accounts
deceased_accounts = (
  month_end_report_v0
  .filter((col('chargedOffReason').isin('DECEASED', 'OTHER')))
)

month_end_report_v2 = (
  month_end_report_v1
  .join(deceased_accounts, ['accountId'], how='left_anti')
)

# MOB >= 4
month_end_report_v4 = (
  month_end_report_v2
  .filter(col('monthOnBook')>=4)
)

exclusions = [# test accounts
							'5efab473ac9014001ecedf4a','5e4dd39ff6aa5c001c78378d','5efab5a6201d3b001ef010e5','5efab6e9201d3b001ef01107','5efab6adac9014001ecedf91','5efab674ac9014001ecedf87','5efab630ac9014001ecedf7e','5efab5ee201d3b001ef010f2','5efab522ac9014001ecedf62','5efab4b8ac9014001ecedf56','5efab42dac9014001ecedf3d','5efab39f201d3b001ef010be','5efab350ac9014001ecedf25','5efab304ac9014001ecedf19','5efab2baac9014001ecedf0b','5efab27bac9014001ecedeff','5efab239ac9014001ecedeeb','5efab1fcac9014001ecedee3','5efab1c3ac9014001eceded9','5efab17f201d3b001ef01083','5efab13bac9014001ecedec9','5efab0eaac9014001ecedeb5','5efab0a4ac9014001ecedeab','5efab067ac9014001ecedea2','5efab026ac9014001ecede99','5efaafe6ac9014001ecede90','5efaafa0201d3b001ef01040','5efaaf56ac9014001ecede6c','5efaaf03ac9014001ecede61','5efaaea0201d3b001ef01022',
							'5efaa921ac9014001eceddbe','5efa8c1f201d3b001ef00edf','5efa8b24201d3b001ef00ece','5e6c119f67030f001d5d50ee','5e6c112e67030f001d5d50e9','5e6c10c639a1d4001c542dd8','5e6c0fef67030f001d5d50e2','5e6c0dfb39a1d4001c542dc9','603dda1d9aff0d848be741c7','603ddd979aff0d3e66e74210','603de0699aff0d3678e74242','603dd816873dbab668dd6989','603dd8be9aff0dbcece741b4','603ddf4a873dba2a86dd69f9','603de15c9aff0d769fe74254','603ddc0a9aff0dfafde741f9','603dde93873dba1ed3dd69e9','603dd537873dba986cdd695d','603de2f69aff0db7f2e7426c','603dd977873dba503edd699b','603ddb4f873dba4f06dd69be','603ddfdc9aff0dd204e7422f','603dd69e873dba5a31dd6970','603dd75c9aff0dcc1be7419e','603ddb4f873dba4f06dd69be',
							# special cases
							'5e608d2d40cd04001de1d93e', # not HBC employee, failed policy but got approved and completed
							'63ebe85b16bb90692b9cafe9', '60744d198db8337ed954a079' # approved but information not recorded
]

# < 30dpd as of month end date - 1st version
final_dataset = (
  month_end_report_v4
  .filter((col('daysPastDue')<30) | (col('daysPastDue').isNull()) & ~(col('daysPastDueBucket')=='B7. Charged-off'))
  .filter(~(col('userId').isin(exclusions)))
)



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 2. Features

# COMMAND ----------

earl_account = (
  spark.table('neo_trusted_analytics.earl_account')
  .filter(col('productTypeName').isin('CATHAY UNSECURED WORLD ELITE CREDIT','HBC UNSECURED STANDARD CREDIT','NEO UNSECURED STANDARD CREDIT','SIENNA UNSECURED STANDARD CREDIT'))
  .filter(col('accountStatus')=='OPEN')
  .filter(col('isAccountChargedOff')=='false')
  

  .withColumn('daysPastDue_', F.when(col('daysPastDue').isNull(),0).otherwise(col('daysPastDue')))

  # Delinquency features
  .withColumn('avg_dpd_last3Mo', F.max('daysPastDue_').over(Window.partitionBy('accountId').orderBy('referenceDate').rowsBetween(-89, 0)))
  .withColumn('max_dpd_last6Mo', F.max('daysPastDue_').over(Window.partitionBy('accountId').orderBy('referenceDate').rowsBetween(-179, 0)))
  .withColumn('avg_dpd_last9Mo', F.max('daysPastDue_').over(Window.partitionBy('accountId').orderBy('referenceDate').rowsBetween(-269, 0)))

  .filter(col('referenceDate')==col('endOfMonthDate_')) # After estimation of delinquency features
  .orderBy(col('accountId'),col('referenceDate'))

  .selectExpr(
    'referenceDate',
    'accountId',
    'productTypeName',
    'brand',
    'monthOnBook',
    'userId',
    'daysPastDue_',
    'avg_dpd_last3Mo',
    'max_dpd_last6Mo',
    'avg_dpd_last9Mo'
  )
)

# COMMAND ----------

stmt_features = (
    spark.table('neo_raw_production.statement_service_credit_card_statements')
    .selectExpr(
          'userid',
          'accountId', 
          'to_date(substring(closingDate,1,10)) as closingdate',
          'summary.account.newbalancecents/100 as statementBalance',
          'summary.account.creditLimitCents/100 as creditLimit',
          'summary.account.interestchargedcents/100 as InterestBalance' 
    )
    .withColumn('month_end_date', last_day(to_date(substring(col('closingDate'),1,10))))
    
    .withColumn('amountOverLimit',
                F.when(col('statementBalance')>col('creditLimit'), col('statementBalance') - col('creditLimit')).otherwise(0)) 
    
    .withColumn('utilization', round(col('statementBalance')/col('creditLimit'),4))

    .withColumn('last3MonthUtilization', F.sum('statementBalance').over(Window.partitionBy('accountId').orderBy(col('month_end_date')).rowsBetween(-2, 0))/ F.sum('creditLimit').over(Window.partitionBy('accountId').orderBy(col('month_end_date')).rowsBetween(-2, 0)))
    
    .select(
      'userid',
      'accountId', 
      'month_end_date',
      'utilization',
      'last3MonthUtilization',
      'amountOverLimit',
      'InterestBalance'
    )
)    

# COMMAND ----------

transactions_features = (
    spark.table('neo_raw_production.bank_service_credit_account_transactions')
    .filter(col('status').isin('CONFIRMED'))
    
    .withColumn('month_end_date', last_day(to_date(substring(col('completedAt'),1,10))))

    .withColumn('deposits_amount', 
                F.when(col('category').isin('TRANSFER_TO_CREDIT_ACCOUNT','PAYMENT_RECEIVED','PAYMENT'),col('amountcents')/100).otherwise(0))
    .withColumn('interest_amount', 
                F.when(col('category').isin('INTEREST'),col('amountcents')/100).otherwise(0))

    .groupBy('accountId', 'userid', 'month_end_date')
    .agg(F.sum('deposits_amount').alias('deposits_amount'),
         F.sum('interest_amount').alias('interest_amount'))
    
    .orderBy(col('accountId'), col('month_end_date'))

    .withColumn('3MoInterestAvg', F.mean('interest_amount').over(Window.partitionBy('accountId').orderBy(col('month_end_date')).rowsBetween(-2, 0)))
    .withColumn('12MoInterestAvg', F.mean('interest_amount').over(Window.partitionBy('accountId').orderBy(col('month_end_date')).rowsBetween(-11, 0)))
)

# COMMAND ----------

creditreport_creditvision = (
    spark.table('neo_raw_production.transunion_creditreport_creditvision')
    .select(
      'user_id','credit_report_date','CVSC100','GO21','AM04','GO152','AT07','RE28')
    
    .withColumn('creditReportDate', to_date(col("credit_report_date").cast("string"), "yyyyMMdd"))
    .withColumn('month', month(col('creditReportDate')))
    .withColumn('year', year(col('creditReportDate')))
  
    # Keep only most recent report by month
    .withColumn('reportOrder', F.row_number().over(Window.partitionBy("user_id","month","year").orderBy("creditReportDate")))
    .withColumn('month_end_date', last_day(col('creditReportDate')))
    .withColumn('max_row', F.max('reportOrder').over(Window.partitionBy('user_id','month_end_date').orderBy(col('month_end_date'))))
    .filter(col('max_row')==col('reportOrder'))
    .drop('reportOrder','max_row','month','year')

    .withColumn('currentScore', col('CVSC100').cast('int'))
    .withColumn('CVSC100', 
                F.when(col('CVSC100')=='000', None)
                .otherwise(col('CVSC100').cast(IntegerType())))
    .orderBy(col('user_id'), col('month_end_date'))

    # Convert to numerical
    .withColumn('AM04', col('AM04').cast(IntegerType()))
    .withColumn('GO21', col('GO21').cast(IntegerType()))
    .withColumn('GO152', col('GO152').cast(IntegerType()))
    .withColumn('AT07', col('AT07').cast(IntegerType()))
    .withColumn('RE28', col('RE28').cast(DoubleType()))

)


# COMMAND ----------

# TU Last non Null Credit Score - Grace's code 

df_transunion_listing = (
  spark.table('neo_raw_production.transunion_creditreport_creditvision')
  .selectExpr(
    "*",
    "to_date(credit_report_date,'yyyyMMdd') as reformatted_report_date"
  )
  .selectExpr(
    "user_id",
    "from_utc_timestamp(to_timestamp(reformatted_report_date),'America/Denver') as last_transunion_report_timestamp",
    "CVSC100 as last_CVSC100"
  )
)

df_transunion_listing_non_nulls = (
  spark.table('neo_raw_production.transunion_creditreport_creditvision')
  .selectExpr(
    "*",
    "to_date(credit_report_date,'yyyyMMdd') as reformatted_report_date"
  )
  .selectExpr(
    "user_id",
    "from_utc_timestamp(to_timestamp(reformatted_report_date),'America/Denver') as last_transunion_report_timestamp",
    "CVSC100 as last_nonNull_CVSC100"
  )
  .filter(~(col('last_nonNull_CVSC100').isNull()))
  .filter(~(col('last_nonNull_CVSC100')==""))
)


df_transunion_calendar_nulls = (
  earl_account
  .join(df_transunion_listing, (earl_account.userId == df_transunion_listing.user_id) & (df_transunion_listing.last_transunion_report_timestamp < earl_account.referenceDate), how = 'leftouter')
  .drop(df_transunion_listing.user_id)
  .withColumn('reportorder',row_number().over(Window.partitionBy(col('accountId'), col('referenceDate')).orderBy(F.desc('last_transunion_report_timestamp'))))
  .filter((col('reportorder')==1))
  .drop('reportorder')
)

df_transunion_calendar_non_nulls = (
  earl_account
  .join(df_transunion_listing_non_nulls, (earl_account.userId == df_transunion_listing_non_nulls.user_id) & (df_transunion_listing_non_nulls.last_transunion_report_timestamp < earl_account.referenceDate), how = 'leftouter')
  .drop(df_transunion_listing_non_nulls.user_id)
  .withColumn('reportorder',row_number().over(Window.partitionBy(col('accountId'), col('referenceDate')).orderBy(F.desc('last_transunion_report_timestamp'))))
  .filter((col('reportorder')==1))
  .drop('reportorder')
  .selectExpr(
    "userId as userId_",
    "referenceDate as referenceDate_",
    "last_nonNull_CVSC100"
  )
)

df_last_nonNull_tu = (
  df_transunion_calendar_nulls
  .join(df_transunion_calendar_non_nulls, (df_transunion_calendar_nulls.userId == df_transunion_calendar_non_nulls.userId_) & (df_transunion_calendar_nulls.referenceDate == df_transunion_calendar_non_nulls.referenceDate_), how = 'left')
  .drop(df_transunion_calendar_non_nulls.userId_)
  .drop(df_transunion_calendar_non_nulls.referenceDate_)

  .withColumn('last_nonNull_CVSC100', col('last_nonNull_CVSC100').cast(IntegerType()))
  .withColumn('last_CVSC100', col('last_CVSC100').cast(IntegerType()))
)

# COMMAND ----------

all_accounts = (
  df_last_nonNull_tu # All accounts and Delinquency features

  # Statements features
  .join(stmt_features, 
        (stmt_features.accountId==df_last_nonNull_tu.accountId) & 
        (stmt_features.userid==df_last_nonNull_tu.userId) &
        (stmt_features.month_end_date==df_last_nonNull_tu.referenceDate), how='left')
  .drop(stmt_features.userid)
  .drop(stmt_features.accountId)
  .drop(stmt_features.month_end_date)
 
 # Transaction features 
 .join(transactions_features, 
        (df_last_nonNull_tu.userId==transactions_features.userid) &
        (df_last_nonNull_tu.accountId==transactions_features.accountId) & 
        (df_last_nonNull_tu.referenceDate==transactions_features.month_end_date), how='left')
  .drop(transactions_features.userid)
  .drop(transactions_features.accountId)
  .drop(transactions_features.month_end_date)

  .join(creditreport_creditvision, (creditreport_creditvision.user_id==earl_account.userId) & (creditreport_creditvision.month_end_date==earl_account.referenceDate), how='left')
  .drop(creditreport_creditvision.user_id)
  .drop(creditreport_creditvision.month_end_date)
  .drop(creditreport_creditvision.creditReportDate)

)

# COMMAND ----------

#display(all_accounts.groupBy('referenceDate','brand').agg(F.count('accountId'),F.mean('last_nonNull_CVSC100')))

# COMMAND ----------

# Backfill - only CRCs features

crcs_list = ['CVSC100', 'AM04', 'GO21', 'GO152', 'AT07', 'RE28']

for i in crcs_list :
  all_accounts = (
    all_accounts
    .withColumn(i+'_backfill',
                F.last(i, ignorenulls=True).over(Window.partitionBy("accountId").orderBy("referenceDate")))
  )

all_accounts = all_accounts.select('referenceDate','accountId','last3MonthUtilization', 
          '3MoInterestAvg','utilization','last_nonNull_CVSC100','InterestBalance','avg_dpd_last3Mo', 
           'max_dpd_last6Mo','AM04_backfill','GO21_backfill','GO152_backfill','deposits_amount',
           'AT07_backfill','amountOverLimit','12MoInterestAvg','avg_dpd_last9Mo','RE28_backfill' )

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 3. Sampling

# COMMAND ----------

model_features = [ 
          'last3MonthUtilization', 
          '3MoInterestAvg', 
          'utilization', 
          'last_nonNull_CVSC100', 
           'InterestBalance', 
           'avg_dpd_last3Mo', 
           'max_dpd_last6Mo',
           'AM04_backfill',
           'GO21_backfill',
           'GO152_backfill',
           'deposits_amount',
           'AT07_backfill',
           'amountOverLimit',
           '12MoInterestAvg',
           'avg_dpd_last9Mo',
           'RE28_backfill' 
           ]

# COMMAND ----------

final = (
  final_dataset
  .join(all_accounts, on=['referenceDate','accountId'], how='left')
  .drop(all_accounts.accountId)
  .drop(all_accounts.referenceDate)
  .select('referenceDate','accountId','brand','bad',*model_features)
)

# COMMAND ----------

#display(final.groupBy('referenceDate','brand').agg(F.count('accountId').alias('accounts'),F.sum('bad').alias('bad')).withColumn('bad_rate', col('bad')/col('accounts')))

# COMMAND ----------

df_enc_pd =  final.filter((col('referenceDate')>='2021-10-31'))


# COMMAND ----------

# Key features for total population
good_num = df_enc_pd.filter(col('bad')==0).count()
bad_num = df_enc_pd.filter(col('bad')==1).count()
total = good_num + bad_num
bad_num_neo = df_enc_pd.filter((col('bad')==1)  & (col('brand')=='NEO')).count()
bad_num_hbc = df_enc_pd.filter((col('bad')==1)  & (col('brand')=='HBC')).count()


# COMMAND ----------

#display(df.groupBy('brand').agg(F.count('accountId').alias('accounts'),F.sum('bad').alias('bad')))

# COMMAND ----------

df = df_enc_pd.orderBy('referenceDate','accountId').selectExpr('bad','brand','referenceDate', *model_features).toPandas()

df.head()

# COMMAND ----------

df.info()

# COMMAND ----------

# split adjusted population data df into train (build) and test (holdout) datasets
from sklearn.model_selection import train_test_split

df['referenceDate'] = pd.to_datetime(df['referenceDate'])

X = df[df.referenceDate <= '2023-05-31']
y = df[df.referenceDate <= '2023-05-31']
y = y.bad

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y, random_state=781263)

# Out-Of-Time Sample
X_oot = df[df.referenceDate > '2023-03-31']
X_oot = X_oot.loc[:, X_oot.columns.isin(model_features)] 

y_oot = df[df.referenceDate > '2023-03-31']
y_oot = y_oot.bad


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 4. Train Model

# COMMAND ----------

# Keep only the required fields for the final model

# Train dataset
X_train_select = X_train.copy()
y_train_select = y_train.copy()

X_train_select = X_train_select.loc[:, X_train_select.columns.isin(model_features)] 

# Test dataset
X_test_select = X_test.copy()
y_test_select = y_test.copy()

X_test_select = X_test_select.loc[:, X_test_select.columns.isin(model_features)] 

# OOT dataset
X_oot_select = X_oot.copy()
y_oot_select = y_oot.copy()

X_oot_select = X_oot_select.loc[:, X_oot_select.columns.isin(model_features)] 

X_oot_select.head(5)


# COMMAND ----------

def get_confusion_matrix_plot(model_used, x, y):
  y_preds = model_used.predict_proba(x)[:, 1]
  y_preds_binary = np.where(y_preds > 0.5, 1, 0)
  cm = confusion_matrix(y, y_preds_binary)
  disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['good', 'bad'])
  fig = plt.figure(figsize=(10,10))
  disp.plot(cmap=plt.cm.Blues, ax=fig.gca())

  tp = cm[1, 1]
  fn = cm[1, 0]
  fp = cm[0, 1]
  tn = cm[0, 0]
  accuracy = (tp + tn) / (tp + fn + fp + tn)
  precision = tp / (tp + fp)
  recall = tp / (tp + fn)
  misclassification = 1 - accuracy
  auc = roc_auc_score(y, y_preds)
  gini = auc*2-1
  return fig, accuracy, precision, recall, misclassification, gini


# COMMAND ----------

y_train_select.info()

# COMMAND ----------


final_model = xgb.XGBClassifier(
  objective= "binary:logistic",
  colsample_bytree= 0.85, 
  subsample =0.85,
  learning_rate= 0.1,
  max_depth= 7,
  min_child_weight =3, 
  gamma= 0.3,
  eval_metric= "auc",
  seed= 2023
)

final_model.fit(X_train_select, y_train_select)

fig, accuracy, precision, recall, misclassification, gini = get_confusion_matrix_plot(final_model, X_train_select, y_train_select)

print("Gini Coefficient: {:.2f}%".format(gini * 100))

# COMMAND ----------

feature_important = final_model.get_booster().get_score(importance_type='gain')
keys = list(feature_important.keys())
values = list(feature_important.values())

data = pd.DataFrame(data=values, index=keys, columns=["Gain"])
data.index.name = "features"
data.nlargest(20, columns="Gain").sort_values(by = "Gain", ascending=True).plot(kind='barh', figsize = (20,10))

# COMMAND ----------

data

# COMMAND ----------



# COMMAND ----------

# Only TU Score
X_train_select_TU_score = X_train_select.last_nonNull_CVSC100

final_model_tu_score = xgb.XGBClassifier(
  objective= "binary:logistic",
  colsample_bytree= 0.85, 
  subsample =0.85,
  learning_rate= 0.1,
  max_depth= 7,
  min_child_weight =3, 
  gamma= 0.3,
  eval_metric= "auc",
  seed= 2023
)

final_model_tu_score.fit(X_train_select_TU_score, y_train_select)

fig, accuracy, precision, recall, misclassification, gini = get_confusion_matrix_plot(final_model_tu_score, X_train_select_TU_score, y_train_select)
  
print("Accuracy: {:.2f}%".format(accuracy * 100))
print("Precision: {:.2f}%".format(precision * 100))
print("Recall: {:.2f}%".format(recall * 100))
print("Misclassification: {:.2f}%".format(misclassification * 100))
print("Gini Coefficient: {:.2f}%".format(gini * 100))

# COMMAND ----------

# auc plot

from sklearn import metrics
import matplotlib.ticker as ticker

# FINAL MODEL
y_pred_proba_train = final_model.predict_proba(X_train_select)
y_pred_proba_test = final_model.predict_proba(X_test_select)
y_pred_proba_oot = final_model.predict_proba(X_oot_select)

fpr_test, tpr_test, _ = metrics.roc_curve(y_test_select, y_pred_proba_test[:, 1])
fpr_train, tpr_train, _ = metrics.roc_curve(y_train_select, y_pred_proba_train[:, 1])
fpr_oot, tpr_oot, _ = metrics.roc_curve(y_oot_select, y_pred_proba_oot[:, 1])

auc_test = metrics.auc(fpr_test, tpr_test)
auc_train = metrics.auc(fpr_train, tpr_train)
auc_oot = metrics.auc(fpr_oot, tpr_oot)

# MODEL only TU Score
y_pred_proba_train_tu = final_model_tu_score.predict_proba(X_train_select_TU_score)
fpr_train_tu, tpr_train_tu, _ = metrics.roc_curve(y_train_select, y_pred_proba_train_tu[:, 1])
auc_train_tu = metrics.auc(fpr_train_tu, tpr_train_tu)

X_test_select_TU_score = X_test_select.last_nonNull_CVSC100
X_oot_select_TU_score = X_oot_select.last_nonNull_CVSC100

y_pred_proba_test_tu = final_model_tu_score.predict_proba(X_test_select_TU_score)
y_pred_proba_oot_tu = final_model_tu_score.predict_proba(X_oot_select_TU_score)

fpr_test_tu, tpr_test_tu, _ = metrics.roc_curve(y_test_select, y_pred_proba_test_tu[:, 1])
fpr_oot_tu, tpr_oot_tu, _ = metrics.roc_curve(y_oot_select, y_pred_proba_oot_tu[:, 1])
auc_test_tu = metrics.auc(fpr_test_tu, tpr_test_tu)
auc_oot_tu = metrics.auc(fpr_oot_tu, tpr_oot_tu)

plt.figure(figsize=(8, 5))
plt.title(f"ROC curve")
# ,\nAUC=(train: {auc_train:.4f},test: {auc_test:.4f}, \noot: {auc_oot:.4f}, train TU_score: {auc_train_tu:.4f},test TU_score: {auc_test_tu:.4f},  OOT TU_score: {auc_oot_tu:.4f}")
plt.plot(fpr_test, tpr_test, label="[Development - Test] V2.1", color = 'green')
plt.plot(fpr_train, tpr_train, label="[Development - Train] V2.1", color = 'orange')
plt.plot(fpr_oot, tpr_oot, label="[Validation - OOT] V2.1", color = 'blue')
plt.plot(fpr_train_tu, tpr_train_tu, label="[Development - Train] TU Score", linestyle = ':', color = 'orange')
plt.plot(fpr_test_tu, tpr_test_tu, label="[Development- Test] TU Score", linestyle = ':', color = 'green')
plt.plot(fpr_oot_tu, tpr_oot_tu, label="[Validation - OOT] TU Score", linestyle = ':', color = 'black')
plt.gca().xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: '{:.0%}'.format(x)))
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

plt.xlabel('False positive rate')
plt.ylabel('True positive rate')
plt.legend(loc='lower right')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 5. Parameter Tuning

# COMMAND ----------



# COMMAND ----------


