# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Normalize hardcheck
spark = SparkSession.builder.getOrCreate()
spark.sql(
  """
    create table neo_views_credit_risk.wk_hardcheck_normalize
    select
  *
from
  (
    select
      hc_id,
      sum(X.balance) as hc_balance,
      sum(X.creditLimit) as hc_creditLimit,
      sum(X.pastDue) as hc_pastDue,
      sum(X.payment) as hc_payment,
      sum(X.plus30) as hc_plus30,
      sum(X.plus60) as hc_plus60,
      sum(X.plus90) as hc_plus90,
      sum(
        case
          when X.type = 'R' then 1
          else 0
        end
      ) as rev_cnt,
      sum(
        case
          when X.type = 'I' then 1
          else 0
        end
      ) as ins_cnt,
      sum(
        case
          when X.type = 'M' then 1
          else 0
        end
      ) as mort_cnt,
      count(*) as trade_cnt
    from
      (
        select
          _id as hc_id,
          explode(details.trades) as X
        from
          neo_raw_production.identity_service_transunion_hard_credit_check_reports
      )
      group by hc_id
  )
  """
)

# COMMAND ----------

#Normalize softcheck

spark.sql(
    """create table neo_views_credit_risk.wk_softcheck_normalize select * from (select sc_id, explode(params) from (SELECT 
          sc_id
          ,MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(accountNetCharacteristics.id, accountNetCharacteristics.value))) params
        FROM ((select _id as sc_id
          ,details.accountNetCharacteristics from neo_raw_production.identity_service_transunion_soft_credit_check_reports
          union select _id as sc_id
          ,details.accountNetCharacteristics from neo_raw_production.application_service_transunion_soft_credit_reports) as sc) LATERAL VIEW INLINE(accountNetCharacteristics) accountNetCharacteristics
        GROUP BY sc_id))
    PIVOT (
          SUM(CAST(value AS INT)) AS crcValue FOR key IN (
      'AM02',
      'AM04',
      'AM07',
      'AM167',
      'AM21',
      'AM216',
      'AM29',
      'AM33',
      'AM34',
      'AM36',
      'AM41',
      'AM42',
      'AM43',
      'AM44',
      'AM57',
      'AM60',
      'AM84',
      'AM91',
      'AT01',
      'AT02',
      'AT07',
      'AT21',
      'AT29',
      'AT33',
      'AT34',
      'AT36',
      'AT60',
      'AT84',
      'BC02',
      'BC04',
      'BC141',
      'BC142',
      'BC143',
      'BC144',
      'BC145',
      'BC147',
      'BC148',
      'BC21',
      'BC33',
      'BC34',
      'BC36',
      'BC60',
      'BC62',
      'BC75',
      'BC76',
      'BC77',
      'BC78',
      'BC79',
      'BC80',
      'BC84',
      'BC85',
      'BC86',
      'BC91',
      'BC94',
      'BR02',
      'BR04',
      'BR60',
      'BR62',
      'BR84',
      'BR91',
      'GO06',
      'GO07',
      'GO11',
      'GO14',
      'GO141',
      'GO148',
      'GO149',
      'GO15',
      'GO151',
      'GO152',
      'GO17',
      'GO21',
      'GO26',
      'GO80',
      'GO81',
      'GO83',
      'GO91',
      'IDXBE01',
      'IDXBE02',
      'IDXBE03',
      'IDXBE04',
      'IDXBE05',
      'IDXBE06',
      'IDXBE07',
      'IDXBE08',
      'IDXBE09',
      'IDXBE10',
      'IDXBE11',
      'IDXBE12',
      'IDXBE13',
      'IDXBE14',
      'IDXBE15',
      'IDXBE16',
      'IDXBE17',
      'IDXBE18',
      'IDXBE19',
      'IDXBE21',
      'IDXBE22',
      'IDXBE23',
      'IDXBE24',
      'IDXBE26',
      'IDXBE27',
      'IDXBE28',
      'IDXBE30',
      'IDXBE31',
      'IDXBE35',
      'IDXBE36',
      'IDXBE38',
      'IDXBE39',
      'IDXBE40',
      'IDXBE42',
      'IDXBE43',
      'IDXBE44',
      'IDXBE45',
      'IDXBE46',
      'IDXBE47',
      'IDXBE48',
      'IDXBE49',
      'IDXBE50',
      'IDXBE51',
      'IDXBE52',
      'IDXBE53',
      'IDXCF191',
      'IDXCF193',
      'IDXCF194',
      'IDXCF237',
      'IDXCF239',
      'IDXFR01',
      'IDXFR02',
      'IDXFR03',
      'IDXFR04',
      'IDXFR05',
      'IDXFR06',
      'IDXFR07',
      'IDXFR08',
      'IDXFR09',
      'IDXFR10',
      'IDXFR100',
      'IDXFR101',
      'IDXFR102',
      'IDXFR103',
      'IDXFR104',
      'IDXFR105',
      'IDXFR106',
      'IDXFR107',
      'IDXFR108',
      'IDXFR109',
      'IDXFR11',
      'IDXFR110',
      'IDXFR111',
      'IDXFR112',
      'IDXFR113',
      'IDXFR114',
      'IDXFR115',
      'IDXFR116',
      'IDXFR117',
      'IDXFR118',
      'IDXFR12',
      'IDXFR122',
      'IDXFR125',
      'IDXFR13',
      'IDXFR130',
      'IDXFR131',
      'IDXFR136',
      'IDXFR138',
      'IDXFR139',
      'IDXFR14',
      'IDXFR146',
      'IDXFR15',
      'IDXFR153',
      'IDXFR16',
      'IDXFR162',
      'IDXFR169',
      'IDXFR17',
      'IDXFR172',
      'IDXFR173',
      'IDXFR174',
      'IDXFR176',
      'IDXFR18',
      'IDXFR184',
      'IDXFR187',
      'IDXFR188',
      'IDXFR19',
      'IDXFR20',
      'IDXFR205',
      'IDXFR206',
      'IDXFR207',
      'IDXFR208',
      'IDXFR209',
      'IDXFR21',
      'IDXFR210',
      'IDXFR211',
      'IDXFR212',
      'IDXFR213',
      'IDXFR214',
      'IDXFR215',
      'IDXFR216',
      'IDXFR217',
      'IDXFR218',
      'IDXFR219',
      'IDXFR22',
      'IDXFR220',
      'IDXFR221',
      'IDXFR222',
      'IDXFR223',
      'IDXFR224',
      'IDXFR225',
      'IDXFR226',
      'IDXFR227',
      'IDXFR228',
      'IDXFR229',
      'IDXFR23',
      'IDXFR230',
      'IDXFR231',
      'IDXFR232',
      'IDXFR233',
      'IDXFR234',
      'IDXFR235',
      'IDXFR236',
      'IDXFR24',
      'IDXFR25',
      'IDXFR26',
      'IDXFR27',
      'IDXFR28',
      'IDXFR29',
      'IDXFR30',
      'IDXFR31',
      'IDXFR32',
      'IDXFR33',
      'IDXFR34',
      'IDXFR35',
      'IDXFR36',
      'IDXFR37',
      'IDXFR38',
      'IDXFR39',
      'IDXFR40',
      'IDXFR41',
      'IDXFR42',
      'IDXFR43',
      'IDXFR44',
      'IDXFR45',
      'IDXFR46',
      'IDXFR47',
      'IDXFR48',
      'IDXFR49',
      'IDXFR50',
      'IDXFR51',
      'IDXFR52',
      'IDXFR53',
      'IDXFR54',
      'IDXFR55',
      'IDXFR56',
      'IDXFR57',
      'IDXFR58',
      'IDXFR59',
      'IDXFR60',
      'IDXFR61',
      'IDXFR62',
      'IDXFR63',
      'IDXFR64',
      'IDXFR65',
      'IDXFR66',
      'IDXFR67',
      'IDXFR68',
      'IDXFR69',
      'IDXFR70',
      'IDXFR71',
      'IDXFR72',
      'IDXFR73',
      'IDXFR74',
      'IDXFR75',
      'IDXFR76',
      'IDXFR77',
      'IDXFR78',
      'IDXFR79',
      'IDXFR80',
      'IDXFR81',
      'IDXFR82',
      'IDXFR83',
      'IDXFR84',
      'IDXFR85',
      'IDXFR86',
      'IDXFR87',
      'IDXFR88',
      'IDXFR89',
      'IDXFR90',
      'IDXFR91',
      'IDXFR92',
      'IDXFR93',
      'IDXFR94',
      'IDXFR95',
      'IDXFR96',
      'IDXFR97',
      'IDXFR98',
      'IDXFR99',
      'IDXID01',
      'IDXID03',
      'IDXID04',
      'IDXID05',
      'IDXID06',
      'IDXID07',
      'IDXID09',
      'IDXID10',
      'IDXID11',
      'IDXID12',
      'IDXID13',
      'IDXID14',
      'IDXID15',
      'IDXID17',
      'IDXID18',
      'IDXID19',
      'IDXID20',
      'IDXID21',
      'IDXID23',
      'IDXID24',
      'IDXID25',
      'IDXID26',
      'IDXID27',
      'IDXID28',
      'IDXID30',
      'IDXID32',
      'IDXID33',
      'IDXID34',
      'IDXID35',
      'IDXID36',
      'IDXID37',
      'IDXSF190',
      'IDXSF191',
      'IDXSF192',
      'IDXSF193',
      'IDXSF194',
      'IDXSF197',
      'IDXSF202',
      'IDXSF237',
      'IDXSF238',
      'IDXSF240',
      'IDXSF241',
      'IDXSF244',
      'IN04',
      'IN60',
      'IN84',
      'MC60',
      'PR09',
      'PR10',
      'PR100',
      'PR11',
      'PR116',
      'PR117',
      'PR119',
      'PR120',
      'PR123',
      'PR124',
      'PR14',
      'PR15',
      'PR21',
      'PR22',
      'PR30',
      'PR41',
      'PR42',
      'PR43',
      'PR44',
      'PR45',
      'PR46',
      'PR47',
      'PR50',
      'PR51',
      'PR52',
      'PR68',
      'PR69',
      'PR70',
      'PR73',
      'PR74',
      'PR75',
      'PR95',
      'PR97',
      'PR98',
      'RE01',
      'RE02',
      'RE03',
      'RE04',
      'RE05',
      'RE06',
      'RE07',
      'RE09',
      'RE28',
      'RE29',
      'RE33',
      'RE336',
      'RE34',
      'RE35',
      'RE37',
      'RE38',
      'RE41',
      'RE42',
      'RE43',
      'RE60',
      'RE61',
      'RE62',
      'RE75',
      'RE76',
      'RE77',
      'RE81',
      'RE82',
      'RE83',
      'RE84',
      'RE91',
      'RR02',
      'RR04',
      'RR60',
      'RR62',
      'RR84',
      'RR91',
      'SD60',
      'SL60'
      )
    )
  """
)

# COMMAND ----------

#Join data without hardcheck - there's much fewer hard check than soft check
spark.sql(
  """
  create table neo_views_credit_risk.wk_appl_model_raw_features_no_hardcheck
    select
      ea.*,
      ms._id as ms_id,
      ms._replicationVersion,
      ms.adjudicationResult,
      ms.applicationId,
      ms.applicationType,
      ms.berbixIdVerificationResult,
      ms.complyAdvantageSanctionsAndPepChecksResult,
      ms.email,
      ms.enstreamAccountIntegrityChecksResult,
      ms.enstreamIdentityVerificationChecksResult,
      ms.fraudVendorCheckResult,
      ms.fraudVendorMetadata,
      ms.hasACompletedAndApprovedApplication,
      ms.iovationDigitalFraudChecksResult,
      ms.kycCheckResult,
      ms.kycDocumentResult,
      ms.prequalificationSinMismatchCheckResult,
      ms.transunionEbvsIdChecksResult,
      ms.transunionHardCreditCheckResult,
      ms.transunionHrfaCheckResult,
      ms.transunionSoftCreditCheckResult,
      ms.userDetails,
      ms.idVerificationResult,
      sc.*,
      usu._id as usu_id,
      usu.ambassadorCode,
      usu.annualIncomeHistory,
      usu.creditScore,
      usu.dateOfBirth,
      usu.deletedAt,
      usu.emails,
      usu.employmentInfo,
      usu.employmentInfoHistory,
      usu.encryptedAnnualIncome,
      usu.encryptedTaxInformation,
      usu.externalId,
      usu.failedLoginAttempts,
      usu.firstName,
      usu.frozenReason,
      usu.housingStatus,
      usu.housingStatusHistory,
      usu.inactiveReason,
      usu.investmentAccount,
      usu.language,
      usu.lastLoginAttempt,
      usu.lastLoginReferralLink,
      usu.lastName,
      usu.locale,
      usu.lockoutExpiresAt,
      usu.monthlyHousingCostCents,
      usu.monthlyHousingCostHistory,
      usu.password,
      usu.personalIdentifiableInformationHistory,
      usu.physicalAddress,
      usu.preferredName,
      usu.previousEmails,
      usu.previousPasswords,
      usu.previousPhone,
      usu.previousPhysicalAddresses,
      usu.products,
      usu.referralLink,
      usu.riskProfile,
      usu.roles,
      usu.softCreditCheckReportId,
      usu.timezone,
      usu.voucherCode,
      usu.mailingAddress,
      usu.partner,
      usu.sin,
      usu.middleName
      from
        neo_raw_production.credit_onboarding_service_credit_applications as ea
        inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms
        inner join neo_views_credit_risk.wk_softcheck_normalize as sc
        inner join neo_raw_production.user_service_users as usu
        on ea.userReportsMetadataSnapshotId = ms._id
        and ms.transunionSoftCreditCheckResult.reportId = sc.sc_id
        and ea.userId = usu._id
  """
)

# COMMAND ----------

#Join hardcheck
spark.sql(
  """
    create or replace table neo_views_credit_risk.wk_appl_model_raw_features_w_hardcheck
      select nhc.*
      , hc.*
    from neo_views_credit_risk.wk_appl_model_raw_features_no_hardcheck as nhc
      inner join neo_views_credit_risk.wk_hardcheck_normalize as hc
      on transunionHardCreditCheckResult.reportId = hc.hc_id
  """
)

# COMMAND ----------

# Create default data

spark.sql(
  """
  create table neo_views_credit_risk.wk_default_list as
    select * from
    (
      SELECT
      applicationId,
      referenceDate,
      brand,
      productTypeName,
      monthOnBook,
      creditFacility,
      1 as isdefault,
      row_number() OVER(
        PARTITION BY accountId
        ORDER BY
          referenceDate
      ) AS n
      FROM
        neo_trusted_analytics.earl_account
      where
      (
        daysPastDue >= 90
        or chargedOffReason not in ("N/A")
      )
    order by
      applicationId,
      referenceDate
    )
  where n = 1
"""
)

# COMMAND ----------

# Create lifetime p&l data

spark.sql(
  """
  create table neo_views_credit_risk.wk_lifetime_pl as
    SELECT
      applicationId,
      max(monthOnBook) as MOB,
      coalesce(max(cumulativeCreditAccountRevenue), 0) as rev,
      coalesce(max(cumulativeCreditAccountRevenue), 0) / max(monthOnBook) as rev_per_m,
      coalesce(max(netCreditChargeOffExpense), 0) as charge_off,
      coalesce(max(netCreditChargeOffExpense), 0) / max(monthOnBook) as charge_off_per_m,
      coalesce(max(cumulativeCreditAccountRevenue),  0) + coalesce(max(netCreditChargeOffExpense), 0) as net_rev,
      (coalesce(max(cumulativeCreditAccountRevenue), 0) + coalesce(max(netCreditChargeOffExpense), 0)) / max(monthOnBook) as net_rev_per_m
    FROM
      neo_trusted_analytics.earl_account
    group by
      applicationId
  """
)

# COMMAND ----------

# Join Data for no hard check

spark.sql(
  """
    CREATE TABLE neo_views_credit_risk.wk_feature_and_target_no_hc
      SELECT
        b.*,
        dl.referenceDate as default_dt,
        dl.brand as earl_brand,
        dl.productTypeName,
        dl.creditFacility,
        coalesce(dl.isdefault, 0) as isdefault,
        DATEDIFF(day, CAST(b.completedAt as date), dl.referenceDate) as day_diff,
        case when day_diff <= 365 then 1 else 0 end as isdefault_1y,
        case when day_diff <= 182 then 1 else 0 end as isdefault_6m,
        pl.rev,
        pl.rev_per_m,
        pl.charge_off,
        pl.charge_off_per_m,
        pl.net_rev,
        pl.net_rev_per_m
      FROM
        neo_views_credit_risk.wk_appl_model_raw_features_no_hardcheck as b
        left join neo_views_credit_risk.wk_default_list as dl
          on b.applicationId = dl.applicationId
        left join neo_views_credit_risk.wk_lifetime_pl as pl
          on b.applicationId = pl.applicationId
  """
)

# COMMAND ----------

# Join Data for hard check

spark.sql(
  """
    CREATE TABLE neo_views_credit_risk.wk_feature_and_target_w_hc
      SELECT
        b.*,
        dl.referenceDate as default_dt,
        dl.brand as earl_brand,
        dl.productTypeName,
        dl.creditFacility,
        coalesce(dl.isdefault, 0) as isdefault,
        DATEDIFF(day, CAST(b.completedAt as date), dl.referenceDate) as day_diff,
        case when day_diff <= 365 then 1 else 0 end as isdefault_1y,
        case when day_diff <= 182 then 1 else 0 end as isdefault_6m,
        pl.rev,
        pl.rev_per_m,
        pl.charge_off,
        pl.charge_off_per_m,
        pl.net_rev,
        pl.net_rev_per_m
      FROM
        neo_views_credit_risk.wk_appl_model_raw_features_w_hardcheck as b
        left join neo_views_credit_risk.wk_default_list as dl
          on b.applicationId = dl.applicationId
        left join neo_views_credit_risk.wk_lifetime_pl as pl
          on b.applicationId = pl.applicationId
  """
)

# COMMAND ----------


