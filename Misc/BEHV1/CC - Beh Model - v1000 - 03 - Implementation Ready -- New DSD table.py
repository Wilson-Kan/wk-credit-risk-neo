# Databricks notebook source
# MAGIC %md
# MAGIC # Unsecured Credit Card - Behavioral Model - v1000 - 03 - Implementation Ready Code

# COMMAND ----------

# Choose month end var here, for Apr 2023 onwards:
# chooseMon = '2023-10-31'
chooseMon = '2024-08-31'

# New below
# saveFileAsReady = neo_views_credit_risk.cc_beh_v1000_pop_ready_202309 
# saveFileAsScore = neo_views_credit_risk.cc_beh_v1000_pop_score_202309 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monthly Batch

# COMMAND ----------

# ########################################################### 
# Function to convert the predicted y (bad rate) to a score 
# ########################################################### 
def get_behaviour_score(bad_rate):
  base_score = 700
  base_odds = 50
  ln_base_odds = np.log(base_odds)
  pdo = 30
  factor_b = pdo/np.log(2)
  offset_a = base_score - (ln_base_odds * factor_b)
  odds = (1/bad_rate)-1
  score = offset_a + (factor_b * np.log(odds))
  return score, odds

def get_confusion_matrix_plot(model_used, x, y):
  y_preds = model_used.predict(x)
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
  auc = roc_auc_score(y, y_preds_binary)
  gini = auc*2-1
  return fig, accuracy, precision, recall, misclassification, gini

# COMMAND ----------

# ####################################################################
# Libraries
# ####################################################################
# sql, pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import  col, to_date, when, concat, year, month, lit, lpad, dayofyear, weekofyear, array, explode, lower, expr, greatest, desc, concat_ws, to_timestamp,avg,row_number
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
# MAGIC ### 1.1. Prepare the monthly data

# COMMAND ----------

db = 'neo_raw_production'
bank_service_savings_accounts = spark.table(f'{db}.bank_service_savings_accounts')
user_service_users = spark.table(f'{db}.user_service_users')
transunion_creditreport_creditvision = spark.table(f'{db}.transunion_creditreport_creditvision')
bank_service_credit_account_transactions = spark.table(f'{db}.bank_service_credit_account_transactions')

db = 'neo_analytics_production'
merchant_category_mapping = spark.table(f'{db}.merchant_category_mapping')

# COMMAND ----------

dfcreditapplications = spark.sql(
  '''
  select
        userid,
        adjudicationReportId,
        transunionSoftCreditReportId,
        Prod_type,
        Cardtype,
        origin,
        applicationcreditscore
    from (
        select
             _id as applicationid,
            userid,
            FROM_UTC_TIMESTAMP(createdAt, 'America/Denver') as applicationdate,
            steps.manualIdentification.data.idProvinceOfIssue as idprovince,
            contactInformation.email,
            steps.verifyIdDetails.data.dateOfBirth as dob,
            status,
            decision,
            coalesce(round(steps.adjudication.data.creditLimitCents*0.01,0),0) as adjCreditLimit,
            steps.verifyIdDetails.data.physicalAddress.province as province,
            steps.verifyIdDetails.data.physicalAddress.postal as postalCode,
            steps.verifyIdDetails.data.physicalAddress.city as city,
            steps.housingStatus.data.housingStatus,
            flaggedformanualreview,
            steps.verifyIdDetails.data.firstName as firstname,
            steps.verifyIdDetails.data.lastName as lastname,
            steps.adjudication.decision as adjdecision,
            coalesce(cast(steps.transunionSoftCreditCheck.data.creditScore as integer), 0) as applicationcreditscore,
            case 
              when flaggedformanualreview = 'true' and flaggedForManualReviewAt is not null then flaggedForManualReviewAt
              when flaggedformanualreview = 'false' and decision = 'APPROVED' and status in ('COMPLETED', 'IN_PROGRESS') and steps.adjudication.data.decidedat is not null then steps.adjudication.data.decidedat
              when decision = 'NO_DECISION' and status in ('IN_PROGRESS','ABANDONED') and flaggedformanualreview = 'false' then 'notapplicable'
            else createdat
            end as appdate,
            steps.adjudication.adjudicationReportId,
            steps.employment.data.employmentStatus,
            steps.employment.data.employmentStartTimeFrame as timeEmployed,
            steps.employment.data.occupation,
            steps.employment.data.fieldOfStudy jobField,
            steps.transunionSoftCreditCheck.transunionSoftCreditReportId,
            steps.housingCost.data.monthlyHousingCostCents/100 as housingcosts,
            steps.address.data.moveInTimeFrame,
--             coalesce(steps.adjudication.data.PreQualifyCreditLimitCents*0.01, 0) as limitPQ,
            'NEOUnsecured' as Prod_type,
            'NEO' as Cardtype,
            'ONLINE' as origin
      from neo_raw_production.application_service_credit_account_applications
      where status = 'COMPLETED' and decision = 'APPROVED'
      )  
      -- where adjCreditLimit >100
  '''
)


dfhudsononlineapplications = spark.sql(
  '''
select
        userid,
        adjudicationReportId,
        transunionSoftCreditReportId,
        Prod_type,
        Cardtype,
        origin,
        applicationcreditscore
    from (
        select
             _id as applicationid,
            userid,
            FROM_UTC_TIMESTAMP(createdAt, 'America/Denver') as applicationdate,
            contactInformation.email,
            steps.verifyIdDetails.data.dateOfBirth as dob,
            status,
            decision,
            coalesce(round(steps.adjudication.data.creditLimitCents*0.01,0),0) as adjCreditLimit,
            steps.verifyIdDetails.data.physicalAddress.province as province,
            steps.verifyIdDetails.data.physicalAddress.postal as postalCode,
            steps.verifyIdDetails.data.physicalAddress.city as city,
            steps.housingStatus.data.housingStatus,
            flaggedformanualreview,
            steps.verifyIdDetails.data.firstName as firstname,
            steps.verifyIdDetails.data.lastName as lastname,
            steps.adjudication.decision as adjdecision,
            coalesce(cast(steps.transunionSoftCreditCheck.data.creditScore as integer), 0) as applicationcreditscore,
            case 
              when flaggedformanualreview = 'true' and flaggedForManualReviewAt is not null then flaggedForManualReviewAt
              when flaggedformanualreview = 'false' and decision = 'APPROVED' and status in ('COMPLETED', 'IN_PROGRESS') and steps.adjudication.data.decidedat is not null then steps.adjudication.data.decidedat
              when decision = 'NO_DECISION' and status in ('IN_PROGRESS','ABANDONED') and flaggedformanualreview = 'false' then 'notapplicable'
            else createdat
            end as appdate,
            steps.adjudication.adjudicationReportId,
            steps.employment.data.employmentStatus,
            steps.employment.data.employmentStartTimeFrame as timeEmployed,
            steps.employment.data.occupation,
            steps.employment.data.fieldOfStudy jobField,
            steps.transunionSoftCreditCheck.transunionSoftCreditReportId,
            steps.address.data.moveInTimeFrame,
            steps.housingCost.data.monthlyHousingCostCents/100 as housingcosts,
            coalesce(steps.adjudication.data.hbcPreQualifyCreditLimitCents*0.01, 0) as limitPQ,
            'HBCUnsecured' as Prod_type,
            'HBC' as Cardtype,
            'ONLINE' as origin
      from neo_raw_production.application_service_hbc_credit_applications
      where status = 'COMPLETED' and decision = 'APPROVED'
    )
    -- where adjCreditLimit > 100
    '''
)


dfhudsonstoreapplications = spark.sql(
  '''
select
        userid,
        adjudicationReportId,
        transunionSoftCreditReportId,
        Prod_type,
        Cardtype,
        origin,
        applicationcreditscore
    from (
        select
            _id as applicationid,
            userid,
            FROM_UTC_TIMESTAMP(createdAt, 'America/Denver') as applicationdate,
            steps.pointOfSaleIdentification.data.idProvinceOfIssue as idprovince,
            contactInformation.email,
            steps.verifyIdDetails.data.dateOfBirth as dob,
            status,
            decision,
            coalesce(round(steps.adjudication.data.creditLimitCents*0.01,0),0) as adjCreditLimit,
            steps.verifyIdDetails.data.physicalAddress.province as province,
            steps.verifyIdDetails.data.physicalAddress.postal as postalCode,
            steps.verifyIdDetails.data.physicalAddress.city as city,
            steps.housingStatus.data.housingStatus,
            flaggedformanualreview,
            steps.verifyIdDetails.data.firstName as firstname,
            steps.verifyIdDetails.data.lastName as lastname,
            steps.adjudication.decision as adjdecision,
            coalesce(cast(steps.transunionSoftCreditCheck.data.creditScore as integer), 0) as applicationcreditscore,
            case 
              when flaggedformanualreview = 'true' and flaggedForManualReviewAt is not null then flaggedForManualReviewAt
              when flaggedformanualreview = 'false' and decision = 'APPROVED' and status in ('COMPLETED', 'IN_PROGRESS') and steps.adjudication.data.decidedat is not null then steps.adjudication.data.decidedat
              when decision = 'NO_DECISION' and status in ('IN_PROGRESS','ABANDONED') and flaggedformanualreview = 'false' then 'notapplicable'
            else createdat
            end as appdate,
            steps.adjudication.adjudicationReportId,
            steps.employment.data.employmentStatus,
            steps.employment.data.employmentStartTimeFrame as timeEmployed,
            steps.employment.data.occupation,
            steps.employment.data.fieldOfStudy jobField,
            steps.transunionSoftCreditCheck.transunionSoftCreditReportId,
            steps.housingCost.data.monthlyHousingCostCents/100 as housingcosts,
            steps.address.data.moveInTimeFrame,
            coalesce(steps.adjudication.data.hbcPreQualifyCreditLimitCents*0.01, 0) as limitPQ,
            'HBCUnsecured' as Prod_type,
            'HBC' as Cardtype,
            'IN_STORE' as origin
     from neo_raw_production.application_service_hbc_instore_credit_applications
     where status = 'COMPLETED' and decision = 'APPROVED'
    )
    -- where adjCreditLimit >100
    '''
)


dfstandardapps = spark.sql(
  '''
  select
        userid,
        adjudicationReportId,
        transunionSoftCreditReportId,
        Prod_type,
        Cardtype,
        origin,
        applicationcreditscore
    from (
     select
        a._id as applicationid,
        a.userId,
        FROM_UTC_TIMESTAMP(a.createdAt, 'America/Denver') as applicationdate,
        a.userInformation.primaryemail as email,
        a.userInformation.dateOfBirth as dob,
        a.status,
        a.decision,
        coalesce(round(creditInformation.creditLimitCents*0.01,0),0) as adjCreditLimit,
        a.userInformation.physicalAddress.province as province,
        a.userInformation.physicalAddress.city as city,
        a.userInformation.physicalAddress.postal as postalCode,
        b.adjudicationResult.input.housing.status housingStatus,
        a.creditInformation.creditScore as applicationcreditscore,
        a.flaggedforreview,
        a.userInformation.firstName as firstname,
        a.userInformation.lastName as lastname,
        b.adjudicationResult.reportid adjudicationReportId,
        b.transunionSoftCreditCheckResult.reportId transunionSoftCreditReportId,
        b.adjudicationResult.input.employment.status  employmentStatus,
        b.adjudicationResult.input.employment.timeAtJob timeEmployed,
        c.employment.jobtitle occupation,
        c.employment.field as jobField,
        a.userInformation.physicalAddress.moveInTimeFrame,
        b.adjudicationResult.input.housing.monthlyCostCents/100 housingcosts,
        case when brand = 'NEO' and type = 'STANDARD' then 'NEOUnsecured'
        when brand = 'NEO' and type = 'SECURED' then 'NEOSecured'
        when brand = 'HBC' and type = 'STANDARD' and origin = 'ONLINE' then 'HBCUnsecured'
        when brand = 'HBC' and type = 'STANDARD' and origin != 'ONLINE' then 'HBCUnsecured'
        when brand = 'HBC' and type = 'SECURED' then 'HBCSecured'
		end as Prod_type,
        brand as Cardtype,
        origin
   from neo_raw_production.credit_onboarding_service_credit_applications a
   left join neo_raw_production.identity_service_user_reports_metadata_snapshots b
      on a.userReportsMetadataSnapshotId = b._id
   left join neo_raw_production.unified_onboarding_service_metadata c
     on a.userid = c.userid
   where status = 'COMPLETED' and decision = 'APPROVED' 
   and type = 'STANDARD'
    )
'''
) 

# COMMAND ----------

users = (
  user_service_users
  .select('*', F.explode('roles').alias('exploded'))
  .groupby('_id', 'roles')
  .agg(
    (F.count('exploded') - 1).alias('#OfProductsInternally'), 
    when(F.array_contains(col('roles'), 'INVESTMENT'), 1).otherwise(0).alias('investmentAccount'),
    when(F.array_contains(col('roles'), 'SAVINGS'), 1).otherwise(0).alias('savingsAccount')
  )
  .withColumnRenamed('roles', 'accountRoles')
  .join(
      (user_service_users.select('_id', 'type')), ['_id'], 'left')
  .withColumnRenamed('_id', 'userid')
  .filter(col('type')!='TEST')
  .drop(col('type'))    )

creditaccount = spark.sql(
  '''
  select userid, _id as creditaccountid, 
  FROM_UTC_TIMESTAMP(createdat, 'America/Denver') as creditOpenedDate, FROM_UTC_TIMESTAMP(closeDate, 'America/Denver') as creditClosedDate
  from neo_raw_production.bank_service_credit_accounts
  '''
)

# COMMAND ----------

apps = (
  dfcreditapplications
  .unionByName(dfhudsononlineapplications)
  .unionByName(dfhudsonstoreapplications)
  .unionByName(dfstandardapps)
    .join(users, ['userid'], 'right')
  .join(creditaccount, ['userid'], 'right')  
  .filter(col('adjudicationReportId').isNotNull())
)
apps.createOrReplaceTempView("apps")

# COMMAND ----------

statements = spark.sql(
'''
  select
    userid,
    last_day_of_month,
    closingdate,
    statementBalance,
    creditLimit,
    revolvingBalance,
    sumRevolving6Mo,
    creditLimit_two,
    creditLimit_three,
    statementBalance_two,
    statementBalance_three,
    round(sum(statementBalance)/sum(creditLimit), 4) utilization_1MLag,
    round(sum(statementBalance_two)/sum(creditLimit_two), 4) utilization_2MLag,
    round(sum(statementBalance_three)/sum(creditLimit_three), 4) utilization_3MLag,
    round(pt_one/pt_two,4)  last6MonthUtilization

  from(
  select 
    userid,
    last_day_of_month,
    closingdate,
    statementBalance,
    creditLimit,
    revolvingBalance,
    sum(revolvingBalance) over(partition by userid ORDER BY last_day_of_month asc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) sumRevolving6Mo,
    sum(statementBalance) over(partition by userid ORDER BY last_day_of_month asc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) pt_one,
    sum(creditLimit) over(partition by userid ORDER BY last_day_of_month asc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) pt_two,
    lag(creditLimit, 1) over(partition by userid order by last_day_of_month asc) creditLimit_two,
    lag(creditLimit, 2) over(partition by userid order by last_day_of_month asc) creditLimit_three,
    lag(statementBalance, 1) over(partition by userid order by last_day_of_month asc) statementBalance_two,
    lag(statementBalance, 2) over(partition by userid order by last_day_of_month asc) statementBalance_three
    
  from(
    select 
      userid, 
      accountstatus, 
      last_day(to_date(from_utc_timestamp(closingdate,'America/Edmonton'))) last_day_of_month,
      to_date(from_utc_timestamp(closingdate,'America/Edmonton')) closingdate,
      summary.account.newbalancecents/100 as statementBalance,
      summary.account.purchasesCents/100 as purchases,
      summary.account.cashAdvancesCents/100 as cashadvance,
      summary.account.interestchargedcents/100 as interestBalance,
      summary.account.creditLimitCents/100 as creditLimit,
      case when summary.account.newbalancecents > summary.account.creditLimitCents then (summary.account.newbalancecents/100 - summary.account.creditLimitCents/100 ) else 0 end as amountOverLimit,
      case when summary.account.paymentsCents < summary.account.previousBalanceCents then (summary.account.previousBalanceCents - summary.account.paymentsCents)/100 else 0 end as revolvingBalance,
      summary.account.paymentsCents/100 paymentsCents, 
      summary.account.rewardsCashedOut.cashedOutCents/100 rewards,
      summary.account.previousBalanceCents/100 previousBalanceCents
    from neo_raw_production.statement_service_credit_card_statements
    )
    group by 1, 2, 3, 4, 5, 6
  )
  group by 1, 2, 3, 4,5,6,7,8,9,10,11,15
'''
)

# COMMAND ----------

dpd_final = spark.sql(
'''
with 

user_data as (
  select distinct userId, accountId
  from neo_trusted_analytics.earl_account
),

dpd0 as (
  select a.*, b.userId as user_id
  from neo_views_credit_risk.old_delinquency_service a
  left join user_data b on a.credit_account_id = b.accountId
),

dpd1 as (
  select 
  distinct
  user_id as userid,
  day_end_days_past_due,
  case when day_end_days_past_due = 1 then 1 ELSE 0 END dpd_ever,
  LAST_DAY(day_end_date) AS last_day_of_month
  from dpd0
),

dpd2 as (
  select
  userid,
  last_day_of_month,
  max(dpd_ever) dpd_ever
  from dpd1
  group by 1,2
),

dpd_final as (
select distinct
  userid,
  last_day_of_month,
  SUM(dpd_ever) OVER (partition by userid ORDER BY last_day_of_month asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) times_DPD_ever
  from dpd2
)

select * from dpd_final
'''
)

# COMMAND ----------

# dpd_final = spark.sql(
# '''
# select distinct
#   userid,
#   last_day_of_month,
#   SUM(dpd_ever) OVER (partition by userid ORDER BY last_day_of_month asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) times_DPD_ever
# from(
#   select
#   userid,
#   last_day_of_month,
#   max(dpd_ever) dpd_ever
# from(
# select 
#   distinct
#   user_id as userid,
#   day_end_days_past_due,
#   case when day_end_days_past_due = 1 then 1 ELSE 0 END dpd_ever,
#   LAST_DAY(day_end_date) AS last_day_of_month
# -- from neo_views_credit_risk.day_end_delinquency_volume_report
# from neo_views_credit_risk.day_end_delinquency_volume_report_aug_31_2023_settled
# )
# group by 1,2
# )
# '''
# )

# COMMAND ----------

purchasepattern = spark.sql(
  '''
select
  userId,
  last_day_of_month,
  shop/all pctShopping,
  health/all pctHealth,
  rest/all pctRestaurant
from(
  select distinct 
      userId
      ,last_day_of_month
      ,max(shoppingAmount) shop
      ,max(healthAmount) health
      ,max(restaurantAmount) rest
      ,max(amountall) all
  from 
    (select 
      distinct
      userid, 
      sum(amountcents/100) OVER (partition by userid ORDER BY createdAt asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) amountall,
      sum(case when spendgroup = 'SHOPPING' then amountcents/100 else 0 end) OVER (partition by userid ORDER BY createdAt asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as shoppingAmount,
      sum(case when spendgroup = 'HEALTH_WELLNESS' then amountcents/100 else 0 end) OVER (partition by userid ORDER BY createdAt asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as healthAmount,
      sum(case when spendgroup = 'FOOD_DRINK' then amountcents/100 else 0 end) OVER (partition by userid ORDER BY createdAt asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as restaurantAmount,
      last_day(FROM_UTC_TIMESTAMP(createdAt, 'America/Edmonton')) last_day_of_month, 
      amountcents/100 as amount
    from neo_raw_production.bank_service_credit_account_transactions t1
      left join neo_analytics_production.merchant_category_mapping t2 on t1.merchantdetails.categoryCode=t2.mcc
      where status = 'CONFIRMED' and type = 'DEBIT' and t1.category = 'PURCHASE' )
      group by 1, 2)
'''
) 

# COMMAND ----------

interest = spark.sql(
  '''
select 
  userid,
  last_day_of_month,
  (SUM(amount) OVER (
    PARTITION BY userid 
    ORDER BY last_day_of_month
    RANGE BETWEEN INTERVAL '11' MONTH PRECEDING AND CURRENT ROW
  ))/12 as 12MoInterestAvg
from(
  select 
    userid, 
    last_day(FROM_UTC_TIMESTAMP(createdAt, 'America/Edmonton')) last_day_of_month,
    last_day(FROM_UTC_TIMESTAMP(createdAt, 'America/Edmonton')) - interval 12 month as lastPeriod,
    to_date(FROM_UTC_TIMESTAMP(createdAt, 'America/Edmonton')) createdat,
    amountcents/100 as amount
  from neo_raw_production.bank_service_credit_account_transactions
  where category = 'INTEREST' 
  )
  '''
)

# COMMAND ----------

credit_file = (
  transunion_creditreport_creditvision
  .select(
    col('user_id').alias('userid'),
    col('CVSC100').alias('currentScore'),
    col('BC60'),
    'GO14',
    'RE34',
    F.last_day(F.to_date(F.from_utc_timestamp(col('createdat'), 'America/Edmonton'))).alias('last_day_of_month')
  )
  .distinct()
  .groupby('userid', 'last_day_of_month')
  .agg(F.first('currentScore').alias('currentScore'), F.first('BC60').alias('BC60'), F.first('GO14').alias('GO14'), F.first('RE34').alias('RE34'))  
)

# COMMAND ----------

dftransunionsoft1 = spark.sql (
  '''
     select softreportid,
            accountnetcharacteristics,
            applicationCreditScore,
            CVSC101,
            tusoftdate
     from(
     select 
           _id as softreportid,
           userid,
           details.accountnetcharacteristics as accountnetcharacteristics,
           details.scoreProduct.score applicationCreditScore,
           CAST(details.scoreProduct.scoreCard AS INTEGER) AS CVSC101,
           (FROM_UTC_TIMESTAMP((createdat) ,'US/Mountain')) as tusoftdate,
           row_number() over(partition by userid order by createdat desc) as reportorder
    from neo_raw_production.identity_service_transunion_soft_credit_check_reports
    )

'''
)
dftransunionsoft1.createOrReplaceTempView("dftransunionsoft1")

soft_check = spark.sql (
  '''
select 
    softreportid transunionSoftCreditReportId,
    RIGHT(params.BC60, 7) app_BC60,
    cast(params.GO14 as integer) app_GO14,
    RIGHT(params.RE34, 7) app_RE34
from(
  select
    softreportid,
    tusoftdate,
    CVSC101,
    map_from_entries(collect_list(struct(tu.id, tu.value))) params
  from
    dftransunionsoft1 lateral view inline(accountnetcharacteristics) tu
  group by
    softreportid, tusoftdate, applicationCreditScore, CVSC101
)
'''
)

# COMMAND ----------

def blank_as_null(x):
  return when(col(x) != "", col(x)).otherwise(None)

# COMMAND ----------

# Final model
apps_final = (
  apps
  .join(statements, ['userid'], 'left')
  .join(interest, ['userid', 'last_day_of_month'], 'left')
  .join(dpd_final, ['userid', 'last_day_of_month'], 'left')  
  .join(purchasepattern, ['userid', 'last_day_of_month'], 'left')
  .join(credit_file, ['userid','last_day_of_month'], 'left')
  .join(soft_check, ['transunionSoftCreditReportId'], 'left')  
  .fillna(0, 
          [
           '12MoInterestAvg', 
           'pctShopping', 'pctRestaurant','pctHealth', 'times_DPD_ever'
          ])

  .filter(col('last_day_of_month')==chooseMon)
  # .filter(col('last_day_of_month')>='2022-01-31')
  # .filter(col('last_day_of_month')<='2022-12-31')
  .withColumn('CVorder', row_number().over(Window.partitionBy("userid").orderBy("last_day_of_month")))
  .withColumn("currentScore", blank_as_null("currentScore"))
  .withColumn("BC60", blank_as_null("BC60"))
  .withColumn("GO14", blank_as_null("GO14"))
  .withColumn("RE34", blank_as_null("RE34"))
  .withColumn("currentScore", when(((col('currentScore').isNull()) & (col('CVorder')==1)),col('applicationcreditscore')).otherwise(col('currentScore')))
  .withColumn("BC60", when(((col('BC60').isNull()) & (col('CVorder')==1)),col('app_BC60')).otherwise(col('BC60')))
  .withColumn("GO14", when(((col('GO14').isNull()) & (col('CVorder')==1)),col('app_GO14')).otherwise(col('GO14')))
  .withColumn("RE34", when(((col('RE34').isNull()) & (col('CVorder')==1)),col('app_RE34')).otherwise(col('RE34')))
  .withColumn("currentScore", F.last("currentScore", ignorenulls=True).over(Window.partitionBy("userid").orderBy("last_day_of_month")))
  .withColumn("BC60", F.last("BC60", ignorenulls=True).over(Window.partitionBy("userid").orderBy("last_day_of_month")))
  .withColumn("GO14", F.last("GO14", ignorenulls=True).over(Window.partitionBy("userid").orderBy("last_day_of_month")))
  .withColumn("RE34", F.last("RE34", ignorenulls=True).over(Window.partitionBy("userid").orderBy("last_day_of_month")))  
  .where((
    (col('creditClosedDate')) >= col('closingdate')) | col('creditClosedDate').isNull())
  .where((col('closingdate') >= col('creditOpenedDate')))
  .drop('adjudicationReportId', 'transunionSoftCreditReportId', 'CVorder', 'app_BC60', 'app_GO14', 'app_RE34', 'accountRoles', '#OfProductsInternally', 'investmentAccount', 'savingsAccount', 'creditLimit_two', 'creditLimit_three')
  )

# COMMAND ----------

# Runtime: 10 min 
sqlContext.sql('DROP TABLE IF EXISTS neo_views_credit_risk.cc_beh_v1000_pop_ready_202408')
apps_final.write.mode("overwrite").partitionBy('last_day_of_month').saveAsTable("neo_views_credit_risk.cc_beh_v1000_pop_ready_202408")

# COMMAND ----------

# %sql
# select count(*) as obs from 
# neo_views_credit_risk.cc_beh_v1000_pop_ready_202311

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Refit the boosting model
# MAGIC This would be the original dataset used to train the model

# COMMAND ----------

exclude_bads = ['608c3e5f446b45f4340c683b',
'6085be2c2ac8c56d8d27269a',
'60cf61ea26e86ec5315cec2b',
'6068af8245ac7c822e60e406',
'6152143910f7a3fbf35a2b4b',
'60a1857dab9b4c2e39903f7d',
'60a800ae4e7a35fcaa4dd1c5',
'6080c11cd172e72818075f59',
'60f74e375b8c553732600359',
'6144cfeb515e2a7b1dfc91b5',
'61257f33ed428fe37117c46b',
'60f5e2a6f3ca58c78ffb56e0',
'6133d22f386e2fdd82c8625d',
'61329371be1bfb609363e170',
'60ec5b812353f2dbe7ab0c58',
'61328ccebe1bfb44a063cac2',
'60a57bb2a798140c17a3d41b',
'60915f0cc482186e39c16c60',
'5fbeb2fce8f4623f2297d20c',
'60fda50af8e11c7eead57aa0',
'6085db327bb32c4264c648a0',
'60ae82d4e94ad71033abadbb',
'61042a5303287ad871842b5b',
'609182432819b33a69a4b8f1',
'60c56f48c5d98d03796146b5',
'60901811541a2a230046df70',
'613d010024b8164ab4383421',
'613661f840cfb9642f3e477e',
'614e462363b5c72adf7b0ac8',
'613e3b82060d33d329be2ee5',
'60d364eb92e467a128e7700f',
'61215121a39e95728c4701da',
'6121488da1109542996d5b9c',
'614f600fcafa4b76ca98d362',
'608ba9b8bcaa10280aa2d24c',
'6078570aefa30b29678a82a8',
'60e8a7d15510761a4539e558',
'60a2aa6ffe67621365edeeb0',
'613e549cdd39852c20064cf6',
'6091bb762c67ff5ba0014262',
'609d8371a1f16a35faac4c2c',
'6082e519064ca191a94f8592',
'614b5d24c5cadc2990f05a3e',
'6068a730b22199403c1b0023',
'61200d7e82a3b24fcf2c67ca',
'61181fd60f714b0c43dd398a',
'609b260b84d6341b89809eb4',
'60d365fc92e4674715e77354',
'6154af3804f892a9060e5439',
'60cbc406c13ba2cf17c8320f',
'604fc40a86e61e626140276b',
'6144f9e5fc94eb7395e95b77',
'6104680c03287a629b84f693',
'60871dfd2ac8c5daa22a2884',
'60fb24b595575a5c09544e6d',
'614cea6e6965d65537c044cc',
'60eb6773bd4e3ae5d16a0ad5',
'6094801cb59036a7ac137da1',
'61465f367fda5ffd87f1d145',
'60c4fe05c5d98d12015fdb30',
'60fb199e354d1b37be89b6f0',
'613d047024b8165a05383e2d',
'60916fd0c4821843cec1a0cf',
'604942be80cc30da3bf3288c',
'60c4fdb0310acf3398b944f7',
'608b209e53ce684cae720086',
'60cfb74586fa9b47a14f4e80',
'610720597a79d127ecb64624',
'611d78acd0de4df1f87466b9',
'614ce869e5b07b53975bd560',
'6116a2508da144e69bf681bc',
'60eb82d02353f2b724a98148',
'60849307b771b63cfb4a3732',
'60ee36cc7bdd561b10cafb73',
'611946f471339be8ca20e956',
'60838661b771b6fc97480844',
'613bdd102ee1fe10d47c0892',
'60998d03b6f8a2819ca42fbf',
'60da26c09b9751d3cd421cba',
'60ccff6784848665de897e11',
'6087356e2ac8c5f4dc2a994d',
'60999926a42e654c0a2db079',
'609ad5b8add5ed5897058a4f',
'60972672cd852e153307c366',
'60b54ba8dda2c524615b77b5',
'60a6bc69e0354aec7bb88a46',
'6086ec312ac8c5379a2945f1',
'608d9ecc0a3c82b78e360abb',
'608ddc2ce2e53d30eb094cfc',
'609d28122c063daadf67595c',
'6100551e8a049e774c04bb27',
'610822d27a79d1031ab885c9',
'607f3fae74628e50376a2914',
'611d96b1d0b9313477e93009',
'60e5ef11364008d0fba3a218',
'6133eafd32f1443b8b0e2483',
'60d21139ef4263bd2104fd01',
'60ca3ef0178693b65a409938',
'60f74d87354d1b30ee7fcadf',
'608de0f50a3c82b56b36b1bd',
'60563aec58b3ec0a9503af34',
'60d60f879c946dde70c7e5d3',
'6110510d19a3fc01d5175704',
'610ff5eb47dd817b6bec4c0f',
'60cd025c4f09d11c67a2f9e6',
'609a78a584d63493a97eb80b',
'60a161dcab9b4c74be8fdb5e',
'609bfb5f855883964281529a',
'610c56e4ce9e64f368a508c8',
'60d22f6da70ee91635db0f04',
'60a968693cbf331689860567',
'60b94cddeb6ae18496e78141',
'605670c4b55c9a63e599ed74',
'60c64430cf2ff51c0fa50f42',
'60ef19f50814c94d1a646777',
'60cff55d8484869b34910b6e',
'60e0a94c1e57354271b405ae',
'606c6066eaebc42a175c7308',
'612130e2680fce15f276de2b',
'614662f54e1e73ba6e105147',
'609c016085588337e281645f',
'60d7a2f835fd29438bfd6611',
'609d87b6d12b3e23a658a011',
'611804546b9a3d8b42a708ab',
'614ddbfbd736945e80f6dafb',
'606cc2c14492975c5af5476d',
'60fdbb0596b5bb871cdaa198',
'608897172042720cb385172b',
'60ccec6fc77d9fe9bfca8a60',
'611aa66a5f0f8235f738331d',
'614259483889920cc447e094',
'6109a5e6f59c2ed12eb49f32',
'608b2a5ebcaa1003c0a1ed1a',
'60ea0f6bc7696e3a3a4fe081',
'61017ae609e754e7ef28647c',
'60a57dd50a1d1749f931e8f8',
'60b01d969cb64b10ae469fcd',
'6013407cc7ca6955c226a74d',
'60847abdee37ce22895dc2ca',
'613d20b1f040f377636b87b5',
'6106ecb59f0bc218cdba1052',
'607dd02821c8cb590d901439',
'60c512fc09466d799d042a13',
'613e58f2862ba9c06cae4077',
'6133b55b32f144ed0f0d43de',
'60d77f7d210a81503588b1a3',
'613be2aca25de1bc58d4098a',
'60f5afe60ad2c7927706d7fa',
'60fc58c895575a4279573aad',
'60f5ed360ad2c7175e07c0e1',
'608f1e44541a2a063b455f1b',
'60a01cebf2da4e26dd96963d',
'613e819cdd3985a75106d5ba',
'60ca16431456710d0b4cf611',
'60ce5b26a9ffe669b04860dc',
'6051803a8c4c680e1dbfd374',
'608af2dcac9f22e38aeaf8fc',
'6115809086b7bd589060356f',
'614f836ccafa4b83d89951ed',
'60c66f72cf2ff54eefa5b28a',
'5fbdef90e8f4624ec597c9c3',
'613e7cbcdd39855a9706c821',
'612e7c91a9a3091698d80691',
'609451905faff627efb2b9e7',
'60e0b2451e5735b02cb42238',
'6068d888fe798a945f38618d',
'61215711a1109530c36d8965',
'613e5c98a41b241f55bfd781',
'60a564c96abfa86ff2afc30d',
'614670acb7a0222a76ad0d91',
'613f8299d85102cc0e77df79',
'6078d0c398e01aa46db0b8a7',
'6130f6de10e0ce43efad18f7',
'60adce64d9e8cdc98e761059',
'607a076d46e26fb60a074714',
'60d24b2413cd9a58f2302e93',
'60983c6fbf99191a18b078f5',
'611feedfc94c1e0147bce7e7',
'61041763044620de565fb6e5',
'5f70dd4fbfe6d229ac12e300',
'61521e5c8c86d5c80002f83a',
'60df70b4eb9b166744105a4b',
'60dfa53e46cb46b14ab4eae2',
'611ecfe9616c59dec4be285e',
'609d809c498c146fb8783b7f',
'60e1fe194833e4046ea764ac',
'608dab430a3c82a9f2362d6c',
'60f34505202a9f4e7d80164f',
'60e09de244e13c162745054e',
'60c7b6ab9843cb3dbb33efdd',
'6150c407cafa4b7d8f9cbb9e',
'60c782f2c5d98d90f265d33c',
'60f7045f5b8c55f6dc5ee15e',
'60aac26fde6e5bdb8348034b',
'60529fbdd807e282ec52558e',
'605cd93e8dee95d69226d5a2',
'612e6e12a9a30921d4d7cf7e',
'607da3312b1970beb8fccc2b',
'60ce388684848647b88c6a3a',
'611aaa76aafad067bd2db04d',
'6147c27c80a9df7961cd06da',
'6150c9b567f4e3691227ef29',
'60fef2ccae5f7052b0a3fc27',
'60886ba9501b2d7ffd4bcd20',
'60fc8ded76ab85525a2b5e7c',
'60e36e2b84999911006aa182',
'5fda4db9374537351cee76c4',
'60e4e6a25e07c0abe5df83af',
'60ca26f636b3fd08781db3a2',
'60e355b55536d18d17443354',
'60f9d9f5c9d8f1685a0b85c7',
'60f35cee202a9f6fa68061bf',
'60bd1f5d793b4a59bc740983',
'6148b750d8e0b42a72d8bd49',
'60c4f601310acf146fb92e14',
'609ec7cbd927067c3d09fb75',
'6100840de9b90572b11a08dd',
'608702b47bb32c26fbc87644',
'613b7e5ea25de130d3d280f2',
'611581f8fb98e87834d4702a',
'61508c28d3e99150b4199678',
'60d64dfb0025ed2d860b1910',
'613b6a47a25de1e868d22fab',
'60945aa885b91035c257c37f',
'60c9172c14567174b44aa703',
'6085e8622ac8c5daca27a9ec',
'60d77550987e68732030306c',
'60da210079b6cf2eee2847eb',
'60fc399895575a247956c6fc',
'612665da40580a5ce75d8292',
'60e485e29f81ee20fb610e7b',
'60846407b771b6707d4993b7',
'6092fd667a01c849207dd105',
'614e0f785c96bd4cdf81dce1',
'6152180b10f7a3ea2c5a3876',
'604a93ec9c6a3b84a29d86df',
'6064d8dcac6763573794f538',
'60959777a212d21bda838c46',
'607b063f575bb466b45b1e93',
'6096d481dd64c8654291ae5a',
'608c86f1533e7cc98ded60c9',
'61104f1e29c24721f8b2ae9c',
'607dcb7c21c8cb1e1b900dfb',
'60cd0750ede98cfb3bc9be7e',
'60805e44449612d1181c8202',
'609d7444ff2f3db64241eacd',
'610461bc03287a378384df02',
'614781acb711301cf120250b',
'613d14502ee1fe53a47f1fc0',
'61019ad48a049e06a1080ec0',
'60fade7b354d1b1c438901dc',
'60919bd42b3a726a5a2385f1',
'6099b56374165df27b9e050d',
'60f5c0edf3ca587deafad62e',
'614264be388992e3af4810e3',
'614b5d76fb1e510931e832d2',
'60d22f1c13cd9a2c8b2fd918',
'60944756b5903689a312dc2b',
'61242cf5e8cb4ec30c9ab6f9',
'6103087c1cee84415d3aab11',
'609bed50855883725781244b',
'61108f5629c24711fdb3859b',
'609da10e748bd3086374c963',
'6078c41598e01ad722b0ae48',
'60f08d5332da39ca04a67dde',
'60a151c4e33ab3f28be66d1b',
'60a16fc8550d78a915a7f6e4',
'60cfaf03c2636202f937d5f6',
'61317e1861194222d9d95b90',
'60f849945b8c55a1e7625500',
'6064c55a1c2c7469be255f9b',
'6088862553158198cb19b25d',
'6068ccc7752bfe815c4e354e',
'610606efcfe8c2181845c9e0',
'6123cb9a2dfd8217c1fe510a',
'613e6e29060d332bc7bedad6']

# COMMAND ----------

##########################################################
# Prepare the training sample to refit the model
##########################################################
db = 'neo_views_credit_risk'
behaviour_score_data_features = spark.table(f'{db}.behaviour_score_data_features')
df = ps.DataFrame(behaviour_score_data_features).to_pandas()

# COMMAND ----------

# %sql
# create table neo_views_credit_risk.behaviour_score_data_features_b as select * from neo_views_credit_risk.behaviour_score_data_features

# COMMAND ----------

df.shape

# COMMAND ----------

# Key features for total population
good_num = df[df.bad==0].shape[0]
bad_num = df[df.bad==1].shape[0]
bad_rate = bad_num/df.shape[0]
if good_num+bad_num==df.shape[0]:
  print('Features for Population:\nGood Users: '+str(good_num)+'\nBad Users: '+str(bad_num)+'\nOriginal Bad Rate: '+str(round(100*bad_rate, 2))+'%')
  print('Recweight for Good: 100%\nRecweight for Bad: 100%')
  print('---------------------------------')

# df_all is the subset of total population with good:bad = 10:1
good_num_selected = 10*bad_num
df_good = df[df.bad==0].sample(n=good_num_selected, random_state=2022) # take goods that are 10 times of bads by SRS
df_bad = df[df.bad==1] # take all bads
df_all = pd.concat([df_good, df_bad]) # subset of df
bad_rate_adj = df_bad.shape[0]/df_all.shape[0] # bad rate for this subset
good_wt = good_num_selected/good_num # recweight, good
bad_wt = df_bad.shape[0]/bad_num # recweight, bad
print('Features for Adjusted Population:\nGood Users: '+str(df_good.shape[0])+'\nBad Users: '+str(df_bad.shape[0])+'\nAdjusted Bad Rate: '+str(round(100*bad_rate_adj, 2))+'%')
print('Recweight for Good: '+str(round(100*good_wt, 2))+'%\nRecweight for Bad: '+str(round(100*bad_wt, 2))+'%')

# The results below align 100% to original model development

# COMMAND ----------

# split adjusted population data df into train (build) and test (holdout) datasets
from sklearn.model_selection import train_test_split

# make sure to apply this rule before fitting the model
X = df_all.loc[:, df_all.columns != 'bad']
y = df_all.bad

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y, random_state=2022)
df_train = pd.concat([X_train, y_train], axis=1)
df_test = pd.concat([X_test, y_test], axis=1)

bad_tr_num = df_train[df_train.bad==1].shape[0]
good_tr_num = df_train[df_train.bad==0].shape[0]
bad_te_num = df_test[df_test.bad==1].shape[0]
good_te_num = df_test[df_test.bad==0].shape[0]

bad_rate_train = bad_tr_num/df_train.shape[0]
bad_rate_test = bad_te_num/df_test.shape[0]

good_wt_tr = good_tr_num/good_num 
bad_wt_tr = bad_tr_num/bad_num 
good_wt_te = good_te_num/good_num 
bad_wt_te = bad_te_num/bad_num

print('Features for Build (Train Data):\nGood Users: '+str(df_train[df_train.bad==0].shape[0])+'\nBad Users: '+str(df_train[df_train.bad==1].shape[0])+'\nTrain Data Bad Rate: '+str(round(100*bad_rate_train, 2))+'%')
print('Recweight for Good: '+str(round(100*good_wt_tr, 2))+'%\nRecweight for Bad: '+str(round(100*bad_wt_tr, 2))+'%')
print('---------------------------------')
print('Features for Holdout (Test Data):\nGood Users: '+str(df_test[df_test.bad==0].shape[0])+'\nBad Users: '+str(df_test[df_test.bad==1].shape[0])+'\nTest Data Bad Rate: '+str(round(100*bad_rate_test, 2))+'%')
print('Recweight for Good: '+str(round(100*good_wt_te, 2))+'%\nRecweight for Bad: '+str(round(100*bad_wt_te, 2))+'%')

# The results below align 100% to original model development

# COMMAND ----------

top_15 = ['applicationcreditscore','currentScore',
'BC60','GO14','pctShopping','times_DPD_ever','utilization_1MLag','RE34','sumRevolving6Mo','utilization_2MLag',
'12MoInterestAvg','last6MonthUtilization', 'utilization_3MLag', 'pctHealth','pctRestaurant']

# COMMAND ----------

# ####################################################
# Keep only the required fields for the final model
# ####################################################
X_train_select = X_train.copy()
y_train_select = y_train.copy()

# Convert fields to numeric
# X_train_select['BC60'] = pd.to_numeric(X_train_select['BC60'])
# X_train_select['RE34'] = pd.to_numeric(X_train_select['RE34'])
# X_train_select['GO14'] = pd.to_numeric(X_train_select['GO14'])

# Error, get the order of fields in line with original development, this has an impact on the result
# X_train_select = X_train_select[['utilization_2MLag','applicationcreditscore','GO14','pctRestaurant',
# '12MoInterestAvg','RE34','last6MonthUtilization','times_DPD_ever','utilization_1MLag',
# 'currentScore','BC60','pctHealth','pctShopping','sumRevolving6Mo','utilization_3MLag']]

# This is the rank order post fitting
# The importrance documented is on all features, not the refitted subset of top 15
# X_train_select = X_train_select[['applicationcreditscore','currentScore','BC60','GO14','pctShopping','times_DPD_ever',

# X_train_select = X_train_select[['applicationcreditscore','currentScore','BC60','GO14','pctShopping','times_DPD_ever',
# 'utilization_1MLag','RE34','sumRevolving6Mo','utilization_2MLag','12MoInterestAvg','last6MonthUtilization',
# 'utilization_3MLag','pctHealth','pctRestaurant']]

# This approach does not order the fields around, keeps the original order
X_train_select = X_train_select.loc[:, X_train_select.columns.isin(top_15)] 

X_train_select.head(5)

# COMMAND ----------

model = xgb.XGBClassifier(
  objective= "binary:logistic",
  colsample_bytree= 0.5, 
  learning_rate= 0.1,
  max_depth= 5, 
  gamma= 0.2,
  eval_metric= "auc",
  seed= 2023
)

model.fit(X_train_select, y_train_select)

feature_important = model.get_booster().get_score(importance_type='weight')
keys = list(feature_important.keys())
values = list(feature_important.values())

data = pd.DataFrame(data=values, index=keys, columns=["score"])
data.index.name = "features"
data.nlargest(15, columns="score").sort_values(by = "score", ascending=True).plot(kind='barh', figsize = (20,10))

# COMMAND ----------

# Temp, confirm score to original model
# Not aligned, see if you can confirm order in original file...
# data.sort_values('score', ascending=False).reset_index()

# COMMAND ----------

fig, accuracy, precision, recall, misclassification, gini = get_confusion_matrix_plot(model, X_train_select, y_train_select)

print("Accuracy: {:.2f}%".format(accuracy * 100))
print("Precision: {:.2f}%".format(precision * 100))
print("Misclassification: {:.2f}%".format(misclassification * 100))
print("Gini Coefficient: {:.2f}%".format(gini * 100))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3. Add the score for a chosen month end

# COMMAND ----------

dfS_Beh_Raw = spark.sql(""" 

with
table_base as (select distinct * from neo_views_credit_risk.cc_beh_v1000_pop_ready_202408)

-- Months on book
,table_mob as (
  	SELECT 
	    userId  
	    ,FROM_UTC_TIMESTAMP((createdAt), 'US/Mountain') AS accountStartDate
	  FROM neo_raw_production.bank_service_credit_accounts)

,table_Join as (
select 
  a.* 
  ,to_date(b.accountStartDate) as accountStartDay
  ,datediff(month, accountStartDate, last_day_of_month)+1 as monthsOnBook
  ,ROW_NUMBER() OVER (PARTITION BY a.userid, last_day_of_month order by last_day_of_month) as rowNum
from table_base as a
  inner join table_mob as b
    on (a.userid=b.userId)    )

select * from table_Join where rowNum = 1 and monthsOnBook >= 4
  """)

# Convert to a panda DF
df_Beh_Raw = ps.DataFrame(dfS_Beh_Raw).to_pandas()

# COMMAND ----------

df_Beh_Prepare = df_Beh_Raw.copy()
####################################################################
# Prepare the final fields for the model to be executed
####################################################################
df_Beh_Prepare['RE34_num'] = pd.to_numeric(df_Beh_Prepare['RE34'])
df_Beh_Prepare['BC60_num'] = pd.to_numeric(df_Beh_Prepare['BC60'])
df_Beh_Prepare['GO14_num'] = pd.to_numeric(df_Beh_Prepare['GO14'])
df_Beh_Prepare['currentScore_num'] = pd.to_numeric(df_Beh_Prepare['currentScore'])
df_Beh_Prepare.drop(['RE34', 'BC60', 'GO14', 'currentScore'], axis=1, inplace=True)
df_Beh_Prepare = df_Beh_Prepare.rename(columns={"RE34_num": "RE34", "BC60_num": "BC60", "GO14_num": "GO14", "currentScore_num": "currentScore"})

df_Beh_X = df_Beh_Prepare[['utilization_2MLag', 'applicationcreditscore', 'GO14', 'pctRestaurant', '12MoInterestAvg', 'RE34', 'last6MonthUtilization', 'times_DPD_ever', 'utilization_1MLag', 'currentScore', 'BC60', 'pctHealth', 'pctShopping', 'sumRevolving6Mo', 'utilization_3MLag']].copy()

# ####################################################################
# Run the fitted model through the new dataset, and add the prediction
# ####################################################################
y_predict_monthly = model.predict_proba(df_Beh_X)
pd_bad_monthly = np.mean(y_predict_monthly, axis=0)

# COMMAND ----------

y_predict_monthly

# COMMAND ----------

# ####################################################################
# Extract the PD from the array
# ####################################################################
y_predict_monthly_use = y_predict_monthly

df_Beh_PD = df_Beh_X.copy()
# Add the PD to the dataset
# df_Beh_PD['bad_rate'] = y_predict_monthly.tolist()

# ####################################################################
# Add NumPy matrix as new columns in DataFrame
# ####################################################################
df_Beh_Final = pd.concat([df_Beh_Raw, pd.DataFrame(y_predict_monthly)], axis=1)
df_Beh_Final = df_Beh_Final.rename(columns={0: "good_rate", 1: "bad_rate"})

# df_Beh_Final.head(10)

# COMMAND ----------

df_Beh_Final.head(10)

# COMMAND ----------

# ####################################################################
# Run the function
# ####################################################################
scores, odds = get_behaviour_score(df_Beh_Final['bad_rate'])

# Add two new columns to the table with the scores and bad rates
df_Beh_Final['score'] = scores
df_Beh_Final['odds'] = odds
df_Beh_Final['scoreBeh'] = df_Beh_Final['score'].round(0)

df_Beh_Final.head(5)

# COMMAND ----------

# ####################################################################
# Ensure results align to development
# ####################################################################
set_bins_list = list([0,500,600,640,680,700,720,740,760,780,800,820,840,850,860,870,900])
df_Beh_Final['scoreBin'] = pd.cut(df_Beh_Final['score'],set_bins_list,right=True)

# Group
df_Beh_Final_Report = df_Beh_Final.groupby(by=['scoreBin', 'last_day_of_month']).agg(observations=('scoreBin', np.size)).reset_index()
df_Beh_Final_Report['perc'] = df_Beh_Final_Report['observations'] / df_Beh_Final_Report['observations'].sum()
# df_Beh_Final_Report

# Plot
fig, ax = plt.subplots(figsize=(14,6))
sns.barplot(data=df_Beh_Final_Report, x='scoreBin', y='perc', edgecolor='k', hue = 'last_day_of_month')
plt.title('Distribution of score bins for month end file')
ax.set_ylabel('Relative percentage of volume')
plt.xticks(rotation=20)
ax.set_xlabel('Score Bin (Group)')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4. Volume review (skip for batch)

# COMMAND ----------

# ####################################################################
# Add NumPy matrix as new columns in DataFrame
# ####################################################################
# dfTrainScore = pd.DataFrame(y_predict_monthly)
# dfTrainScore = dfTrainScore.rename(columns={0: "good_rate", 1: "bad_rate"})

# ####################################################################
# Run the function
# ####################################################################
# scoresT, oddsT = get_behaviour_score(dfTrainScore['bad_rate'])

# Add two new columns to the table with the scores and bad rates
# dfTrainScore['score'] = scoresT
# dfTrainScore['odds'] = oddsT
# dfTrainScore['scoreBeh'] = dfTrainScore['score'].round(0)

# COMMAND ----------

# ####################################################################
# Ensure results align to development
# ####################################################################
# set_bins_list = list([0,500,600,640,680,700,720,740,760,780,800,820,840,850,860,870,900])
# dfTrainScore['scoreBin'] = pd.cut(dfTrainScore['score'],set_bins_list,right=True)

# Group
# dfTrainScore_Report = dfTrainScore.groupby(by=['scoreBin']).agg(observations=('scoreBin', np.size)).reset_index()
# dfTrainScore_Report['perc'] = dfTrainScore_Report['observations'] / dfTrainScore_Report['observations'].sum()

# Plot
# fig, ax = plt.subplots(figsize=(14,6))
# sns.barplot(data=dfTrainScore_Report, x='scoreBin', y='perc', color='#8CB3BB', edgecolor='k')
# plt.title('Distribution of score bins in training set')
# ax.set_ylabel('Relative percentage of volume')
# plt.xticks(rotation=20)
# ax.set_xlabel('Score Bin (Group)')

# COMMAND ----------

# print('Bad rate on development set:',dfTrainScore['bad_rate'].mean().round(4))
# print('Bad rate on monthly file:',df_Beh_Final['bad_rate'].mean().round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5. Save the scored population

# COMMAND ----------

# MAGIC %md
# MAGIC These 4 variables should always be present in output
# MAGIC - model score, shoiwing the final score
# MAGIC - model version, indicating which version of the model is being used to produce the score
# MAGIC - model type, application or behavioral
# MAGIC - model scope, which portfolio has this model been deemed fit for

# COMMAND ----------

# df_Beh_Final.head(5)

# COMMAND ----------

df_Beh_Save = df_Beh_Final.copy()
df_Beh_Save.drop(columns = {'score', 'creditClosedDate', 'closingdate', 'creditClosedDate'}, inplace=True)

# Override score if MOB<=3, to default of -1
df_Beh_Save['modelScore'] = np.where(df_Beh_Save['monthsOnBook']<=3, -1, df_Beh_Save['scoreBeh'])
# df_Beh_Save.rename(columns = {'finScore' : 'modelScore'}, inplace=True)
df_Beh_Save['modelVersion'] = '1.0.0.0'
df_Beh_Save['modelType'] = 'behavioral'
df_Beh_Save['modelScope'] = 'creditCard'
df_Beh_Save = df_Beh_Save[df_Beh_Save['monthsOnBook']>=4]
df_Beh_Save.head(3)

# COMMAND ----------

# print(pd.__version__)

# COMMAND ----------

df_Beh_Save_Subset = df_Beh_Save[['userid', 'last_day_of_month', 'accountStartDay', 'monthsOnBook', 'modelScore', 'modelVersion', 'modelType', 'modelScope']]
df_Beh_Save_Subset['modelScore'] = df_Beh_Save_Subset['modelScore'].astype('int')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
mySchema = StructType([ StructField("userid", StringType(), True)\
                       ,StructField("last_day_of_month", DateType(), True)\
                       ,StructField("accountStartDay", DateType(), True)\
                       ,StructField("monthsOnBook", IntegerType(), True)\
                       ,StructField("modelScore", IntegerType(), True)\
                       ,StructField("modelVersion", StringType(), True)\
                       ,StructField("modelType", StringType(), True)\
                       ,StructField("modelScope", StringType(), True)\
                        ])

# dfS_Beh_Scored = spark.createDataFrame(df_Beh_Save_Subset)

import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items
dfS_Beh_Scored = spark.createDataFrame(df_Beh_Save_Subset)

# COMMAND ----------

sqlContext.sql('DROP TABLE IF EXISTS neo_views_credit_risk.cc_beh_v1000_pop_score_202408')
dfS_Beh_Scored.write.mode("overwrite").saveAsTable("neo_views_credit_risk.cc_beh_v1000_pop_score_202408")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   last_day_of_month
# MAGIC   ,count(*) as obs
# MAGIC from neo_views_credit_risk.cc_beh_v1000_pop_score_202408
# MAGIC WHERE monthsOnBook>=4
# MAGIC   group by last_day_of_month
# MAGIC   order by last_day_of_month

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   userid
# MAGIC   ,count(*) as obs
# MAGIC from neo_views_credit_risk.cc_beh_v1000_pop_score_202311
# MAGIC   group by userid
# MAGIC     having count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6. Historical Report

# COMMAND ----------

# dfS_AllScores = spark.sql(""" 

# WITH
# table_source as (
#   -- select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202301
# --  select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202302
# -- UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202303
# -- select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202304
#  select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202305
# UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202306
# UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202307
# UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202308
# UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202309
# UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202310
# UNION select * from neo_views_credit_risk.cc_beh_v1000_pop_score_202311
# )

# select 
#   userid
#   ,last_day_of_month
#   ,modelScore
# from table_source

# """)

# # Convert to pandas DF for easy charting
# df_AllScores = ps.DataFrame(dfS_AllScores).to_pandas()

# COMMAND ----------

# # ####################################################################
# # Ensure results align to development
# # ####################################################################
# set_bins_list = list([0,450,500,560,590,620,650,710,740,770,800,860,900]) 

# dfPlot = df_AllScores.copy()
# dfPlot['scoreBin'] = pd.cut(dfPlot['modelScore'],set_bins_list,right=True)

# # Group
# dfPlot_Report = dfPlot.groupby(by=['scoreBin', 'last_day_of_month']).agg(observations=('scoreBin', np.size)).reset_index()
# dfPlot_Report_T = dfPlot_Report.groupby(by=['last_day_of_month']).agg(total=('observations', np.sum)).reset_index()
# dfPlot_Report = dfPlot_Report.merge(dfPlot_Report_T, on = 'last_day_of_month')
# dfPlot_Report['perc'] = dfPlot_Report['observations'] / dfPlot_Report['total']
# dfPlot_Report

# # Plot
# fig, ax = plt.subplots(figsize=(20,8))
# sns.barplot(data=dfPlot_Report, x='scoreBin', y='perc', edgecolor='k', hue = 'last_day_of_month', palette = 'rainbow')
# plt.title('Distribution of score bins')
# ax.set_ylabel('Relative percentage of volume')
# plt.xticks(rotation=20)
# ax.set_xlabel('Score Bin (Group)')

# COMMAND ----------


# # Parameters
# PDO = 30
# refScore = 700
# refOdds = 50

# # Convert score to PD
# df_AllScores['modelPD'] = 1 / (1 + (refOdds * (2 ** ((df_AllScores['modelScore']-refScore)/PDO))))


# df_AllScores['modelRG'] = np.select([ 
#   df_AllScores.modelPD<=0.00012,
#   df_AllScores.modelPD.between(0.00012,0.00017, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00017,0.00024, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00024,0.00034, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00034,0.00048, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00048,0.00067, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00067,0.00095, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00095,0.00135, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00135,0.0019, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.0019,0.00269, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00269,0.00381, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00381,0.00538, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00538,0.00761, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.00761,0.01076, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.01076,0.01522, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.01522,0.02153, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.02153,0.03044, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.03044,0.04305, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.04305,0.06089, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.06089,0.08611, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.08611,0.12177, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.12177,0.17222, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.17222,0.24355, inclusive = 'right'),
#   df_AllScores.modelPD.between(0.24355,0.34443, inclusive = 'right'),  
#   df_AllScores.modelPD>0.34443 
#   ],list(np.arange(1, 1+25)),  default=99)

  
# # Group
# dfPlot = df_AllScores.copy()
# dfPlot_Report = dfPlot.groupby(by=['modelRG', 'last_day_of_month']).agg(observations=('modelRG', np.size)).reset_index()
# dfPlot_Report_T = dfPlot_Report.groupby(by=['last_day_of_month']).agg(total=('observations', np.sum)).reset_index()
# dfPlot_Report = dfPlot_Report.merge(dfPlot_Report_T, on = 'last_day_of_month')
# dfPlot_Report['perc'] = dfPlot_Report['observations'] / dfPlot_Report['total']
# dfPlot_Report

# # Plot
# fig, ax = plt.subplots(figsize=(20,8))
# sns.barplot(data=dfPlot_Report, x='modelRG', y='perc', edgecolor='k', hue = 'last_day_of_month', palette = 'rainbow')
# plt.title('Distribution of risk grades (! model needs a recalibration)')
# ax.set_ylabel('Relative percentage of volume') 
# ax.set_xlabel('Risk Grade')
# ax.tick_params(labelsize = 13)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Union of all scores

# COMMAND ----------

# %sql
# -- This is a once off piece of code to create the base table
# create or replace table neo_views_credit_risk.cc_beh_v1000_pop_score using delta as (
# with
# table_append AS (   
# 	select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_2022
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202301
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202302
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202303
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202304
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202305
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202306
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202307
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202308
# 	UNION select userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202309 
# ) 	
# select * from table_append
# )


# COMMAND ----------

# %sql
# select 
#   count(*) as obs
#   ,min(monthEnd) as minM
#   ,max(monthEnd) as maxM 
# from neo_views_credit_risk.cc_beh_v1000_pop_score

# COMMAND ----------

# %sql
# ALTER TABLE neo_views_credit_risk.cc_beh_v1000_pop_score SET TBLPROPERTIES (delta.enableChangeDataFeed = true); 

# COMMAND ----------

# %sql
# -- Keep this running for the month in question until the last day to fill in this table
# MERGE INTO neo_views_credit_risk.cc_beh_v1000_pop_score AS Target
# USING (SELECT userId, last_day_of_month as monthEnd, modelScore FROM neo_views_credit_risk.cc_beh_v1000_pop_score_202311) AS Source
# ON Source.userId = Target.userId and Source.monthEnd = Target.monthEnd

# --------------------
# -- For Inserts
# --------------------
# WHEN NOT MATCHED BY Target THEN
#   INSERT (userId, monthEnd, modelScore) 
#   VALUES (Source.userId, Source.monthEnd, Source.modelScore)
