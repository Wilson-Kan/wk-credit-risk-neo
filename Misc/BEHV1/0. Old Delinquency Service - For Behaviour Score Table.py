# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col, to_date, when, year, month, lpad, lit, row_number,concat, datediff, current_date, greatest, date_sub, date_add, pandas_udf, PandasUDFType, expr, unix_timestamp, weekofyear, to_timestamp, add_months # sql functions
from pyspark.sql.types import *
from datetime import datetime, timedelta # time measurement functions
import pytz # timezone
from pyspark.sql import Window # partition and perform functions on this "window" of data
#import databricks.koalas as ks # allows pandas on apache spark api
import sys
import pandas as pd

# COMMAND ----------

db = 'neo_raw_production' # core database
db_credit_risk = 'neo_views_credit_risk'
db_trusted = 'neo_trusted_production'
db_trusted_risk = 'neo_trusted_risk'
db_trusted_finance = 'neo_trusted_finance'
#rundate =  datetime.strftime(datetime.now(pytz.timezone('US/Mountain')), '%Y-%m-%d') # current date and time; time of run
#last30days =  datetime.strftime(datetime.strptime(rundate,'%Y-%m-%d') - timedelta(30), '%Y-%m-%d') # date and time of today less 30 days - where are these used

# COMMAND ----------

df_daily_calendar = (
  spark.table(f'{db_credit_risk}.day_end_dates')
  #.filter(col('day_end_date') > '2024-05-25')
  #.filter(col('day_end_date') < '2024-01-01')
  #.filter(col('day_end_date') <= '2023-12-31')
  .filter(col('day_end_date') >= '2024-06-28')
)

# COMMAND ----------

# CREDIT REASONS
credit_reasons = ['DELINQUENCY','DECEASED','BANKRUPTCY','CONSUMER_PROPOSALS','FORECLOSURE','SETTLEMENTS','CREDIT_COUNSELLING_SOLUTIONS']

# FRAUD REASONS
fraud_reasons = ['FIRST_PARTY','SECOND_PARTY','THIRD_PARTY','CHARGE_BACK','IDENTITY_THEFT','SYNTHETIC_ID','ACCOUNT_TAKE_OVER','FRIENDLY_FRAUD','MANIPULATION','MULTIPLE_IMPRINT','BUST_OUT','MODIFICATION_OF_PAYMENT_ORDER','POLICY_ABUSE','REWARD_FRAUD','PAYMENT_RISK','TRANSACTION_LAUNDERING','ELDER_ABUSE']

# OTHER REASONS
other_reasons = ['OTHER', 'SYSTEM_ERROR']

# COMMAND ----------

# Step 1: Account Information (Including Charge-Off)

df_accounts = (
  spark.table(f'{db}.credit_account_service_credit_accounts')
  .selectExpr(
    # credit account and associated identifiers
    "_id as credit_account_id",
    "userId as user_id",
    "securedCreditDepositAccountId as secured_credit_deposit_account_id",
    "creditLimitCents*0.01 as credit_limit",
    # account state
    "from_utc_timestamp(to_timestamp(createdAt),'America/Denver') as credit_account_creation_timestamp",
    "from_utc_timestamp(to_timestamp(closeDate),'America/Denver') as credit_account_closure_timestamp", # will also need a credit_account_charge_off_timestamp
    "status", #OPEN, CLOSED, PENDING
    "closeNote as close_note",
    "closeReason as close_reason"
  )
)

df_secured = (
  spark.table(f'{db}.bank_service_secured_credit_deposit_accounts')
  .filter(col('status')=='OPEN')
  .selectExpr(
    "creditAccountId as credit_account_id",
    "status as secured_deposit_status"
  )
)

df_accounts_all = (
  df_accounts
  .join(df_secured,['credit_account_id'],how='left')
  .drop(df_secured.credit_account_id)
)
  
# df_all_chargeoffs = (
#   spark.table(f'{db_credit_risk}.allChargeoffs')
#   .selectExpr(
#     "accountId as credit_account_id",
#     "ChargeoffCategory as charge_off_category",
#     "COType as charge_off_type",
#     "AutoCOReason as auto_charge_off_reason",
#   )
# )

system_error_chargeoff_account_ids = ['616b30a1fbfc915a0b471cdd']
# slack link: https://neofinancial.slack.com/archives/G01HK3K5VMZ/p1667339558007009

df_bank_service_charge_offs = (
  spark.table(f'{db}.bank_service_credit_account_charged_off_details')
  .withColumn("chargedOffReason_Adjusted",
             F.when(col('accountId').isin(system_error_chargeoff_account_ids), "SYSTEM_ERROR")
              .otherwise(col('chargedOffReason'))
             )
  .selectExpr(
    "accountId as credit_account_id",
    "from_utc_timestamp(chargedOffAt, 'America/Denver') as charge_off_details_date",
    "chargedOffReason_Adjusted as charge_off_reason"
  )
)

df_chargeoffs = (
  df_bank_service_charge_offs
  # df_all_chargeoffs
  # .join(df_bank_service_charge_offs, df_all_chargeoffs.credit_account_id == df_bank_service_charge_offs.credit_account_id, how = 'leftouter')
  # .drop(df_bank_service_charge_offs.credit_account_id)
  .withColumn("charge_off_type",
              F.when(col("charge_off_reason").isin(credit_reasons), "CREDIT")
               .when(col("charge_off_reason").isin(fraud_reasons), "FRAUD")
               .when(col("charge_off_reason").isin(other_reasons), "OTHER")
               .otherwise("OTHER - NOT CLASSIFIED")
              )
)

df_accounts_chargeoff = (
  df_accounts_all
  .join(df_chargeoffs, ['credit_account_id'], how = 'left')
)

df_accounts_calendar = (
  df_daily_calendar
  .join(df_accounts_chargeoff, df_accounts_chargeoff.credit_account_creation_timestamp < df_daily_calendar.day_end_timestamp, how = 'left')
  .filter(~col('credit_account_id').isNull())
  # below code will need to be updated when charge-off date is available (currently assumes the close date is the charge-off date)
  .withColumn('day_end_status',
              F.when(col('credit_account_closure_timestamp') >= df_daily_calendar.day_end_timestamp, 'OPEN')
               .otherwise(col('status'))
             )
  .drop('status')
  .orderBy('day_end_timestamp','credit_account_id')
)

#print((df_accounts_calendar.count(), len(df_accounts_calendar.columns)))


# COMMAND ----------

# Step 2: User Information

listtestusers = ['5efab473ac9014001ecedf4a','5e4dd39ff6aa5c001c78378d','5efab5a6201d3b001ef010e5','5efab6e9201d3b001ef01107','5efab6adac9014001ecedf91','5efab674ac9014001ecedf87','5efab630ac9014001ecedf7e','5efab5ee201d3b001ef010f2','5efab522ac9014001ecedf62','5efab4b8ac9014001ecedf56','5efab42dac9014001ecedf3d','5efab39f201d3b001ef010be','5efab350ac9014001ecedf25','5efab304ac9014001ecedf19','5efab2baac9014001ecedf0b','5efab27bac9014001ecedeff','5efab239ac9014001ecedeeb','5efab1fcac9014001ecedee3','5efab1c3ac9014001eceded9','5efab17f201d3b001ef01083','5efab13bac9014001ecedec9','5efab0eaac9014001ecedeb5','5efab0a4ac9014001ecedeab','5efab067ac9014001ecedea2','5efab026ac9014001ecede99','5efaafe6ac9014001ecede90','5efaafa0201d3b001ef01040','5efaaf56ac9014001ecede6c','5efaaf03ac9014001ecede61','5efaaea0201d3b001ef01022','5efaa921ac9014001eceddbe','5efa8c1f201d3b001ef00edf','5efa8b24201d3b001ef00ece','5e6c119f67030f001d5d50ee','5e6c112e67030f001d5d50e9','5e6c10c639a1d4001c542dd8','5e6c0fef67030f001d5d50e2','5e6c0dfb39a1d4001c542dc9']

listtestcards = ['603dda1d9aff0d848be741c7','603ddd979aff0d3e66e74210','603de0699aff0d3678e74242',
                 '603dd816873dbab668dd6989','603dd8be9aff0dbcece741b4','603ddf4a873dba2a86dd69f9',
                 '603de15c9aff0d769fe74254','603ddc0a9aff0dfafde741f9','603dde93873dba1ed3dd69e9',
                 '603dd537873dba986cdd695d','603de2f69aff0db7f2e7426c','603dd977873dba503edd699b',
                 '603ddb4f873dba4f06dd69be','603ddfdc9aff0dd204e7422f','603dd69e873dba5a31dd6970',
                 '603dd75c9aff0dcc1be7419e','603ddac29aff0debd6e741d0','603de25d873dba2571dd6a1b',
                 '603ddb4f873dba4f06dd69be'
                ]

df_users = (
  spark.table(f'{db}.user_service_users')
  .selectExpr(
    # user and associated identifiers
    "_id as user_id",
    # user characteristics
    "type", #CUSTOMER, EMPLOYEE, TEST
    "frozenReason as user_frozen_reason",
    "status as user_status" #ACTIVE, INACTIVE, FROZEN
  )
  .withColumn('user_type',
             F.when(((col('user_id').isin(listtestusers)) | (col('user_id').isin(listtestcards))), 'TEST')
              .otherwise(col('type'))
             )
  .drop('type')
)

df_users_calendar = (
  df_accounts_calendar
  .join(df_users,['user_id'], how = 'left')
  .selectExpr(
    "*",
    "to_date(credit_account_creation_timestamp) as credit_account_creation_date"
  )
  .withColumn('credit_account_type',
             F.when(~col('secured_credit_deposit_account_id').isNull(), 'SECURED')
              .when(col('secured_deposit_status')=='OPEN', 'SECURED')
              .when(col('user_type')=='TEST', 'TEST')
              .when(col('close_note').like('%Test%'), 'TEST')
              .when(((col('credit_account_creation_date') == '2020-06-30') & (col('credit_limit') == 2)), 'TEST')
              .otherwise('UNSECURED')
             )
  .drop(df_users.user_id)
)

#print((df_users_calendar.count(), len(df_users_calendar.columns)))

# COMMAND ----------

# Step 3: Latest Statement Information

df_statements = (
  spark.table(f'{db}.statement_service_credit_card_statements')
  .selectExpr(
    # account and associated identifiers
    "accountId as credit_account_id",
    "userId as statement_user_id",
    # statement dates
    "from_utc_timestamp(to_timestamp(closingDate),'America/Denver') as statement_close_timestamp",
    # statement characteristics
    "summary.account.creditLimitCents*0.01 as statement_credit_limit",
    "summary.account.newBalanceCents*0.01 as statement_total_balance",
    "summary.account.generalInterestCents*0.01 as statement_general_interest"
  )
)

df_statements_listing = (
  df_daily_calendar
  .join(df_statements, df_statements.statement_close_timestamp < df_daily_calendar.day_end_timestamp, how = 'left')
  .withColumn('max_statement_close_timestamp', F.max(col('statement_close_timestamp')).over(Window.partitionBy('day_end_timestamp','credit_account_id')))
  .filter(col('statement_close_timestamp') == col('max_statement_close_timestamp'))
  .drop('max_statement_close_timestamp')
  .selectExpr(
    "*",
    "credit_account_id as statement_credit_account_id",
    "day_end_date as statement_day_end_date",
    "day_end_timestamp as statement_day_end_timestamp"
  )
  .drop('max_statement_close_timestamp', 'day_end_date', 'day_end_timestamp', 'credit_account_id')
)

df_statements_calendar = (
  df_users_calendar
  .join(df_statements_listing, (df_users_calendar.credit_account_id == df_statements_listing.statement_credit_account_id) & (df_users_calendar.day_end_timestamp == df_statements_listing.statement_day_end_timestamp), how = 'leftouter')
  .drop(df_statements_listing.statement_day_end_timestamp)
  .drop(df_statements_listing.statement_day_end_date)
)

#print((df_statements_calendar.count(), len(df_statements_calendar.columns)))

# COMMAND ----------

df_ledger_summary_balance = (
  spark.table(f'{db}.ledger_service_credit_account_balance_summaries')
  .selectExpr(
    "accountRef",
    "from_utc_timestamp(transactionDate, 'America/Edmonton') as recordedAt",
    "(totalBalanceCents / 100) :: decimal(12,2) as ledger_summary_total_balance",
    "(totalBalanceCents / 100) :: decimal(12,2) as ledger_transaction_balance",
    "(settledBalanceCents / 100) :: decimal(12,2) as ledger_summary_settled_balance"
  )
)

df_ledger_summary_balance_calendar = (
  #df_ledger_transaction_balance_calendar
  df_statements_calendar
  #.join(df_ledger_summary_balance, (df_ledger_transaction_balance_calendar.credit_account_id == df_ledger_summary_balance.accountRef) & (df_ledger_transaction_balance_calendar.day_end_timestamp > df_ledger_summary_balance.recordedAt), how = 'left')
  .join(df_ledger_summary_balance, (df_statements_calendar.credit_account_id == df_ledger_summary_balance.accountRef) & (df_statements_calendar.day_end_timestamp > df_ledger_summary_balance.recordedAt), how = 'left')
  .withColumn('reportorder', row_number().over(Window.partitionBy('credit_account_id', 'day_end_timestamp').orderBy(F.desc('recordedAt'))))
  .filter("reportorder = 1")
  .drop("reportorder")
  .drop(df_ledger_summary_balance.accountRef)
  .drop(df_ledger_summary_balance.recordedAt)
)

# COMMAND ----------

# Step 5a: Identify Delinquencies with Payment Issues

df_delinquency_payment_issues = (
  spark.table(f'{db_credit_risk}.delinquency_payment_issue_list')
)

df_delinquency_all = (
  spark.table(f'{db}.delinquency_service_delinquencies')
)

#print(df_delinquency_all.count())

df_delinquency_revised_close_date = (
  df_delinquency_all
  .join(df_delinquency_payment_issues, df_delinquency_all._id == df_delinquency_payment_issues.delinquency_id, how = 'left')
  .withColumn('max_run_date', F.max('run_date').over(Window.partitionBy('run_date')))
  .filter((col('run_date')==col('max_run_date')) | col('run_date').isNull())
  .drop('run_date', 'max_run_date')
  .drop(df_delinquency_payment_issues.delinquency_id)
  .drop(df_delinquency_payment_issues.credit_account_id)
  .drop(df_delinquency_payment_issues.delinquency_service_date_opened)
  .drop(df_delinquency_payment_issues.proxy_delinquency_date_closed)
  .drop(df_delinquency_payment_issues.starting_amount)
  .withColumn('delinquency_proxy_date_used',
             F.when(col('proxy_delinquency_close_timestamp').isNull(),0)
              .otherwise(1)
             )
  .selectExpr(
    "_id",
    "accountId",
    "dateOpened",
    "case when delinquency_proxy_date_used = 0 then from_utc_timestamp(to_timestamp(dateClosed),'America/Denver') else proxy_delinquency_close_timestamp end as dateClosed",
    "deletedAt",
    "originalDelinquentAmountCents",
    "currentDelinquentAmountCents",
    "delinquency_proxy_date_used"
  ) 
)

#print(df_delinquency_revised_close_date.count())

# COMMAND ----------

# Step 5: Delinquency Information

df_delinquency_listing = (
  df_delinquency_revised_close_date
  .filter(col('deletedAt').isNull())
  .selectExpr(
    "accountId as delinquency_credit_account_id",
    "from_utc_timestamp(to_timestamp(dateOpened),'America/Denver') as delinquency_open_timestamp",
    "dateClosed as delinquency_close_timestamp", # already converted to MST in a previous table
    "originalDelinquentAmountCents*0.01 as original_delinquent_amount",
    "currentDelinquentAmountCents*0.01 as current_delinquent_amount",
    "delinquency_proxy_date_used",
    "datediff(MILLISECOND, from_utc_timestamp(to_timestamp(dateOpened),'America/Denver'), dateClosed) as milliseconds_past_due"
  )
  .withColumn('days_past_due_180',
             #F.when((datediff(MILLISECOND, F.col('delinquency_close_timestamp'), F.col('delinquency_open_timestamp'))) >= 15638400000, 1)
             F.when(col('milliseconds_past_due') >= 15638400000, 1)
              .when((col('milliseconds_past_due') > 15552000000) & (col('current_delinquent_amount') > 0), 1)
              .otherwise(0)
             )
  .withColumn('min_open_dates_180', F.min(col('delinquency_open_timestamp')).over(Window.partitionBy('delinquency_credit_account_id', 'days_past_due_180')))
  .withColumn('min_open_date_180',
             F.when(col('days_past_due_180')==1, col('min_open_dates_180'))
              .otherwise(lit(current_date()))
             )
  .withColumn('phantom_delinquency_threshold', F.min(col('min_open_date_180')).over(Window.partitionBy('delinquency_credit_account_id')))
  .filter(col('delinquency_open_timestamp') <= col('phantom_delinquency_threshold'))
  .drop('min_open_dates_180', 'min_open_date_180', 'phantom_delinquency_threshold')
)

charge_off_reasons = ['DELINQUENCY','DECEASED','BANKRUPTCY','CONSUMER_PROPOSALS','FORECLOSURE','SETTLEMENTS','CREDIT_COUNSELLING_SOLUTIONS']

df_delinquency_phantom = (
  df_delinquency_listing
  .join(df_statements_calendar, (df_delinquency_listing.delinquency_credit_account_id == df_statements_calendar.credit_account_id) &  (df_delinquency_listing.delinquency_open_timestamp < df_statements_calendar.day_end_timestamp), how = 'left')
  .withColumn('phantom_delinquency_indicator',
             F.when(((col('statement_credit_account_id').isNull()) & (col('statement_credit_limit').isNull())), 1)
              .when(col('charge_off_reason').isin(charge_off_reasons) & (col('delinquency_open_timestamp') >= col('charge_off_details_date')) & (col('day_end_timestamp') > col('charge_off_details_date')), 1)
              .when(col('charge_off_reason').isin(charge_off_reasons) & (col('delinquency_open_timestamp').isNull()) & (col('day_end_timestamp') > col('charge_off_details_date')), 1)
              .otherwise(0)
             )
  .filter(col('phantom_delinquency_indicator')==0)
  .filter(~col('delinquency_open_timestamp').isNull())
  .selectExpr(
    "delinquency_credit_account_id",
    "day_end_timestamp as  delinquency_day_end_timestamp",
    "day_end_date as  delinquency_day_end_date",
    "delinquency_open_timestamp",
    "delinquency_close_timestamp",
    "days_past_due_180",
    "delinquency_proxy_date_used",
    "milliseconds_past_due",
    "current_delinquent_amount"
  )
)

df_delinquency_calculation = (
  df_delinquency_phantom
  .filter(((col('delinquency_close_timestamp').isNull()) | (col('delinquency_close_timestamp') >= col('delinquency_day_end_timestamp')) | (col('days_past_due_180') == 1))) # filter for delinquencies open as at calendar date
  .withColumn('day_end_delinquency_close_timestamp',
              F.when(((col('delinquency_close_timestamp').isNull())), col('delinquency_day_end_timestamp'))
              .otherwise(col('delinquency_close_timestamp'))
             )
  .withColumn('min_delinquency_open_timestamp',F.min(col('delinquency_open_timestamp')).over(Window.partitionBy('delinquency_day_end_timestamp','delinquency_credit_account_id')))
  .filter(col('delinquency_open_timestamp') == col('min_delinquency_open_timestamp'))
  .withColumn('day_end_days_past_due', (datediff(F.col('delinquency_day_end_timestamp'),F.col('delinquency_open_timestamp'))))
  .selectExpr(
    "*",
    "datediff(MILLISECOND, delinquency_open_timestamp, delinquency_day_end_timestamp) as day_end_milliseconds_past_due"
  )
  .withColumn('day_end_raw_delinquency_bucket',
              F.when((col('day_end_days_past_due') >=   0) & (col('day_end_days_past_due') <  30), '2. 000 - 029')
               .when((col('day_end_days_past_due') >=  30) & (col('day_end_days_past_due') <  60), '3. 030 - 059')
               .when((col('day_end_days_past_due') >=  60) & (col('day_end_days_past_due') <  90), '4. 060 - 089')
               .when((col('day_end_days_past_due') >=  90) & (col('day_end_days_past_due') < 120), '5. 090 - 119')
               .when((col('day_end_days_past_due') >= 120) & (col('day_end_days_past_due') < 150), '6. 120 - 149')
               .when((col('day_end_days_past_due') >= 150) & (col('day_end_days_past_due') < 181), '7. 150 - 180') # Charge offs occur at end of 180th day
               .when(((col('day_end_days_past_due') >= 181) & (col('current_delinquent_amount') > 0)), '8. 180+')
               .otherwise('ERROR - INVALID DELINQUENCY BUCKET')
             )
  .drop('min_delinquency_open_timestamp')
)

manual_charge_off_categories = ['RECLASSIFIED_TO_FRAUD','FRAUD_MANUAL','FRAUD_LIST']

df_delinquency_calendar = (
  df_ledger_summary_balance_calendar
  .join(df_delinquency_calculation, (df_ledger_summary_balance_calendar.credit_account_id == df_delinquency_calculation.delinquency_credit_account_id) & (df_ledger_summary_balance_calendar.day_end_timestamp == df_delinquency_calculation.delinquency_day_end_timestamp), how = 'leftouter')
  .drop(df_delinquency_calculation.delinquency_day_end_timestamp)
  .drop(df_delinquency_calculation.delinquency_day_end_date)
  .drop(df_delinquency_calculation.delinquency_credit_account_id)
  
  .selectExpr(
    "*",
    "date_add(delinquency_open_timestamp, 181) as delinquency_charge_off_date"
  )
  
  # Define charge off date for reporting purposes
  .withColumn('reporting_charge_off_date',
              
            # Case 1: Account is charged off but not recognized as charged off in new charge off table (ie it was a previous manual charge off)
              # F.when((col('charge_off_category').isin(manual_charge_off_categories)) & (col('charge_off_details_date').isNull()), col('credit_account_closure_timestamp'))
              
              #  .when((col('charge_off_category') == 'CREDIT_MANUAL') & ((col('charge_off_reason') != 'DELINQUENCY') | (col('charge_off_reason').isNull())), col('credit_account_closure_timestamp'))
              
            # Case 2: Account has gone more than 180 DPD but isn't formally charged off
              #  .when((col('charge_off_category') == 'Closed, not chargedoff but >180dpd'), col('delinquency_charge_off_date'))
              
            # Case 3: Account was charged off past the point when they went 180 DPD or before it appears to be 180 DPD
               F.when((col('charge_off_reason') == 'DELINQUENCY') & ((datediff(col('charge_off_details_date'), col('delinquency_open_timestamp')) > 181) | (datediff(col('charge_off_details_date'), col('delinquency_open_timestamp')) < 180)), col('delinquency_charge_off_date'))
              
            # Case 4: Account was charged off due to delinquency incorrectly (likely due to soft deleted delinquencies) - these should not have a charge off date, unless they subsequently charged off (should be captured in Case 3 if so)
               .when((col('charge_off_reason') == 'DELINQUENCY') & ((col('delinquency_open_timestamp').isNull()) | (datediff(col('charge_off_details_date'), col('delinquency_open_timestamp')) < 180)), None)
              
            # Case 5: Trust the new charge off table
               .otherwise(col('charge_off_details_date'))
              
             )
  
  .drop('delinquency_charge_off_date')
  
  # Define charge off status as at day end date
  .withColumn('day_end_reporting_charge_off_status',
              
            # Case 1: Charge off date is after the day end date, or charge off has yet to occur in general
             F.when((col('reporting_charge_off_date') >= col('day_end_timestamp')) | (col('reporting_charge_off_date').isNull()), 'NOT CHARGED OFF')
              
            # Case 2: Account is charged off prior to day end date
              .otherwise('CHARGED OFF')
              
             )
  
  # Define charge off type as at day end date
  .withColumn('day_end_reporting_charge_off_type',
              
             # Case 1: Charged Off
              F.when((~col('charge_off_type').isNull()) & (col('day_end_reporting_charge_off_status') == 'CHARGED OFF'), col('charge_off_type'))
              
             # Case 2: Not Charged Off
               .otherwise(None)
              
             )
  
  # Define charge off category as at day end date
  # .withColumn('day_end_reporting_charge_off_category',
              
  #            # Case 1: Charged Off
  #             F.when((~col('charge_off_category').isNull()) & (col('day_end_reporting_charge_off_status') == 'CHARGED OFF'), col('charge_off_category'))
              
  #            # Case 2: Not Charged Off
  #              .otherwise(None)
              
  #            )
  
  # Define charge off reason as at day end date
  .withColumn('day_end_reporting_charge_off_reason',
              
              # Case 1: Charged Off
              F.when((~col('charge_off_reason').isNull()) & (col('day_end_reporting_charge_off_status') == 'CHARGED OFF'), col('charge_off_reason'))
              
             # Case 2: Not Charged Off
               .otherwise(None)
              
             )
  
  # Define auto charge off reason as at day end date
  
  # .withColumn('day_end_reporting_auto_charge_off_reason',
              
  #            # Case 1: Charged Off
  #             F.when((~col('auto_charge_off_reason').isNull()) & (col('day_end_reporting_charge_off_status') == 'CHARGED OFF'), col('auto_charge_off_reason'))
              
  #            # Case 2: Not Charged Off
  #              .otherwise(None)
              
  #            )
  
  # Define delinquency bucket as at day end date
  
  .withColumn('day_end_reporting_delinquency_bucket',
              
              
             # Case 1: Test accounts
              F.when(col('credit_account_type') == 'TEST', col('credit_account_type'))
              
             # Case 2: If account is charged off as at day end date, bucket it in its charge off type
               .when(col('day_end_reporting_charge_off_status') == 'CHARGED OFF', col('day_end_reporting_charge_off_type'))
               
             # Case 3: Account is not delinquent and no statement has been received
               .when((col('day_end_raw_delinquency_bucket').isNull()) & (col('statement_credit_account_id').isNull()) & (col('day_end_status') == 'OPEN'), '1b. CURRENT - NO STATEMENT RECEIVED')
               
             # Case 4: Account is open, not delinquent and has received a statement
               .when((col('day_end_raw_delinquency_bucket').isNull()) & (col('day_end_status') == 'OPEN') & (~col('statement_credit_account_id').isNull()), '1a. CURRENT - STATEMENT RECEIVED')
               
             # Case 5: Account is closed, not delinquent, but is carrying a balance
               .when((col('day_end_raw_delinquency_bucket').isNull()) & (col('day_end_status') == 'CLOSED') & (~col('statement_credit_account_id').isNull()) & (col('ledger_transaction_balance') > 0), '1c. CURRENT - CLOSED - CARRYING A BALANCE')
               
             # Case 6: Account is closed, not delinquent, but is not carrying a balance
               .when((col('day_end_raw_delinquency_bucket').isNull()) & (col('day_end_status') == 'CLOSED') & (~col('statement_credit_account_id').isNull()) & ((col('ledger_transaction_balance') <= 0) | (col('ledger_transaction_balance').isNull())), '1d. CURRENT - CLOSED - NULL, ZERO OR NEGATIVE BALANCE')
              
             # Case 7: Account is current but doesn't fall into cases 3-6
               .when((col('day_end_raw_delinquency_bucket').isNull()), '1e. CURRENT - OTHER')
               
             # Case 8: Account is delinquent
               .otherwise(col('day_end_raw_delinquency_bucket'))
               
              )
  
  .drop('day_end_raw_delinquency_bucket','day_end_delinquency_close_timestamp','charge_off_category','auto_charge_off_reason', 'days_past_due_180', 'milliseconds_past_due','current_delinquent_amount')
  
)


# COMMAND ----------

old_delinquency_service = (
  df_delinquency_calendar
  .selectExpr(
    "credit_account_id",
    "day_end_date",
    "day_end_timestamp",
    "ledger_summary_settled_balance",
    "day_end_days_past_due",
    "reporting_charge_off_date",
    "day_end_reporting_delinquency_bucket"
  )
)

# COMMAND ----------

old_delinquency_service_archive = (
  spark.table(f'{db_credit_risk}.old_delinquency_service')
  .filter(col('day_end_date') < '2024-06-28')
  .selectExpr(
    "credit_account_id",
    "day_end_date",
    "day_end_timestamp",
    "ledger_summary_settled_balance",
    "day_end_days_past_due",
    "reporting_charge_off_date",
    "day_end_reporting_delinquency_bucket"
  )
)

old_delinquency_service_new = (
  old_delinquency_service
  .selectExpr(
    "credit_account_id",
    "day_end_date",
    "day_end_timestamp",
    "ledger_summary_settled_balance",
    "day_end_days_past_due",
    "reporting_charge_off_date",
    "day_end_reporting_delinquency_bucket"
  )
)

old_delinquency_service = (
  old_delinquency_service_archive
  .union(old_delinquency_service_new)
)


# COMMAND ----------

old_delinquency_service.write.mode('overwrite').saveAsTable('neo_views_credit_risk.old_delinquency_service')




# COMMAND ----------

test = (
  spark.table(f'{db_credit_risk}.old_delinquency_service')
)

# COMMAND ----------


