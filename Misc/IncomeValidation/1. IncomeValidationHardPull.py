# Databricks notebook source
start_from_pickle = False

# COMMAND ----------

from pyspark.sql import SparkSession

if not start_from_pickle:
    spark = SparkSession.builder.getOrCreate()

    start_data = spark.sql(
        """
    select
    applicationId,
    cast(trad.balance as float) as hc_balance,
    cast(trad.creditLimit as float) as hc_creditLimit,
    cast(trad.highCredit as float) as hc_highCredit,
    TO_DATE(trad.dateOpened, 'yyyyMMdd') as hc_dateOpened,
    TO_DATE(trad.dateLastActivity, 'yyyyMMdd') as hc_dateLastActivity,
    cast(trad.pastDue as int) as hc_pastDue,
    cast(trad.payment as float) as hc_payment,
    trad.type as hc_type,
    ageBand
    ,subProductName
    ,applicationDecision
    ,applicationCompletedAt_mt
    ,employmentStatus
    ,creditScoreBand
    ,moveInTimeFrame
    ,cardType
    ,jobTitle
    ,currentCity
    ,currentProvince
    ,productName
    ,personalIncomeBand
    ,personalIncomeNumeric
    ,row_number() over (partition by applicationId order by TO_DATE(trad.dateOpened, 'yyyyMMdd') desc) as rank
  from
    (
      select
        eea.applicationId
        ,eea.ageBand
        ,eea.subProductName
        ,eea.applicationDecision
        ,eea.applicationCompletedAt_mt
        ,eea.employmentStatus
        ,eea.creditScoreBand
        ,eea.moveInTimeFrame
        ,eea.cardType
        ,eea.jobTitle
        ,eea.currentCity
        ,eea.currentProvince
        ,eea.productName
        ,eea.personalIncomeBand
        , case when eea.personalIncomeBand = 'a. $0 - $20,000' then 10
        when eea.personalIncomeBand = 'b. $20,000 - $40,000' then 30
        when eea.personalIncomeBand = 'c. $40,000 - $60,000' then 50
        when eea.personalIncomeBand = 'd. $60,000 - $80,000' then 70 
        when eea.personalIncomeBand = 'e. $80,000 - $100,000' then 90
        when eea.personalIncomeBand = 'f. $100,000 - $120,000' then 110
        when eea.personalIncomeBand = 'g. $120,000 - $140,000' then 130
        when eea.personalIncomeBand = 'h. $140,000+' then 150
        else 0 end as personalIncomeNumeric
        ,rea.applicationMetadataId
        ,ms.transunionHardCreditCheckResult.reportId
        ,explode(hc.details.trades) as trad
      from
        neo_trusted_analytics.earl_application as eea
        inner join neo_raw_production.credit_onboarding_service_credit_applications as rea on eea.applicationMetadataId = rea.applicationMetadataId
        inner join neo_raw_production.identity_service_user_reports_metadata_snapshots as ms on rea.userReportsMetadataSnapshotId = ms._id
        inner join neo_raw_production.identity_service_transunion_hard_credit_check_reports as hc on ms.transunionHardCreditCheckResult.reportId = hc._id
    )
    order by
      applicationId,
      hc_dateOpened desc
    """
    )

    start_data.createOrReplaceTempView("start_data_view")

    base_info = spark.sql(
        """
    select distinct applicationId,
      ageBand
      ,subProductName
      ,applicationDecision
      ,applicationCompletedAt_mt
      ,employmentStatus
      ,creditScoreBand
      ,moveInTimeFrame
      ,cardType
      ,jobTitle
      ,currentCity
      ,currentProvince
      ,productName
      ,personalIncomeBand
      ,personalIncomeNumeric 
    from start_data_view
    """
    )

    ins_info = spark.sql(
        """
      select applicationId as ins_applicationId,
      count(*) as ins_count,
      sum(hc_payment) as ins_payment,
      sum(hc_pastDue) as ins_pastDue,
      sum(hc_highCredit) as ins_highCredit,
      sum(hc_creditLimit) as ins_creditLimit,
      sum(hc_balance) as ins_balance
      from start_data_view
      where hc_type = 'I'
      group by applicationId
    """
    )

    rev_info = spark.sql(
        """
      select applicationId as rev_applicationId,
      count(*) as rev_count,
      sum(hc_payment) as rev_payment,
      sum(hc_pastDue) as rev_pastDue,
      sum(hc_highCredit) as rev_highCredit,
      sum(hc_creditLimit) as rev_creditLimit,
      sum(hc_balance) as rev_balance
      from start_data_view
      where hc_type = 'R'
      group by applicationId
    """
    )

    mtg_info = spark.sql(
        """
      select applicationId as mtg_applicationId,
      count(*) as mtg_count,
      sum(hc_payment) as mtg_payment,
      sum(hc_pastDue) as mtg_pastDue,
      sum(hc_highCredit) as mtg_highCredit,
      sum(hc_creditLimit) as mtg_creditLimit,
      sum(hc_balance) as mtg_balance
      from start_data_view
      where hc_type = 'M'
      group by applicationId
    """
    )

    use_this_data = (
        base_info.join(
            ins_info, base_info.applicationId == ins_info.ins_applicationId, "left"
        )
        .join(rev_info, base_info.applicationId == rev_info.rev_applicationId, "left")
        .join(mtg_info, base_info.applicationId == mtg_info.mtg_applicationId, "left")
    )

# COMMAND ----------

if not start_from_pickle:
    from pyspark.sql.functions import sum, count, mean, lower, col, when

    print(f"DataFrame Rows count : {use_this_data.count()}")
    filt1 = use_this_data.filter(
        ~(
            (use_this_data.employmentStatus == "UNEMPLOYED")
            & (use_this_data.personalIncomeNumeric > 100)
        )
    )
    print(f"DataFrame Rows count : {filt1.count()}")
    filt2 = filt1.filter(
        ~(
            (filt1.ageBand.isin(["a. Under 18", "b. 18-29", "c. 30-39"]))
            & (filt1.employmentStatus == "RETIRED")
        )
    )
    print(f"DataFrame Rows count : {filt2.count()}")
    filt3 = filt2.filter(
        ~(
            (filt2.ageBand.isin(["e. 50-59", "f. 60+"]))
            & (filt2.employmentStatus == "STUDENT")
        )
    )
    print(f"DataFrame Rows count : {filt3.count()}")
    filt4 = filt3.filter(
        ~((filt3.personalIncomeNumeric > 80) & (filt3.employmentStatus == "STUDENT"))
    )
    print(f"DataFrame Rows count : {filt4.count()}")
    res_data = filt4.filter(~(filt4.ageBand.isin(["a. Under 18"])))
    print(f"DataFrame Rows count : {res_data.count()}")

    res_data = res_data.withColumn("currentCity", lower(col("currentCity")))
    res_data = res_data.withColumn("currentProvince", lower(col("currentProvince")))
    res_data = res_data.withColumn("jobTitle", lower(col("jobTitle")))

# COMMAND ----------

if not start_from_pickle:
    display(
        res_data.groupBy("employmentStatus")
        .agg(count("creditScoreBand"),mean("personalIncomeNumeric"))
        .sort("employmentStatus")
    )
    display(res_data.groupBy("ageBand").mean("personalIncomeNumeric").sort("ageBand"))
    display(
        res_data.groupBy("currentCity")
        .agg(count("currentCity"), mean("personalIncomeNumeric"))
        .sort("currentCity")
    )
    display(
        res_data.groupBy("creditScoreBand")
        .agg(count("creditScoreBand"), mean("personalIncomeNumeric"))
        .sort("creditScoreBand")
    )
    display(
        res_data.groupBy("jobTitle")
        .agg(count("jobTitle"), mean("personalIncomeNumeric"))
        .sort("jobTitle")
    )
    display(
        res_data.groupBy("currentProvince")
        .agg(count("currentProvince"), mean("personalIncomeNumeric"))
        .sort("currentProvince")
    )

# COMMAND ----------

if not start_from_pickle:

    from pyspark.sql.types import StructType, StructField, StringType, FloatType

    city_data = [
        ("brampton", 49.83666365),
        ("calgary", 63.97696857),
        ("edmonton", 59.8451708),
        ("hamilton", 59.63115222),
        ("laval", 55.99052324),
        ("london", 54.2262099),
        ("markham", 61.74782054),
        ("mississauga", 60.01967268),
        ("montréal", 56.80429961),
        ("ottawa", 66.1188625),
        ("scarborough", 47.93456543),
        ("surrey", 56.99853587),
        ("toronto", 60.67959172),
        ("vancouver", 65.54070184),
        ("winnipeg", 55.12220108),
        ("ELSE", 62.45785929),
    ]

    schema = StructType(
        [
            StructField("currentCity", StringType()),
            StructField("cityScore", FloatType()),
        ]
    )

    city_df = spark.createDataFrame(data=city_data, schema=schema)

    cvs_data = [
        ("b. Sub Prime (300 to 639)", 52.5062716521323),
        ("c. Near-Prime (640 to 719)", 52.2745089819104),
        ("d. Prime (720 to 759)", 50.1696097006441),
        ("e. Prime+ (760 to 799)", 61.6382877834296),
        ("f. Super Prime (800+)", 69.2252668647518),
        ("ELSE", 51.3406735751344),
    ]

    schema = StructType(
        [StructField("creditBand", StringType()), StructField("cvsScore", FloatType())]
    )

    cvs_df = spark.createDataFrame(data=cvs_data, schema=schema)

    job_data = [
        ("accountant", 76.0465116279069),
        ("administrative assistant", 59.1382899009241),
        ("cashier", 34.8260801746997),
        ("customer service representative", 45.7623251661541),
        ("executive/senior management", 103.228875209848),
        ("manager", 80.7214086784655),
        ("nurse", 77.897186376137),
        ("other", 63.949683593147),
        ("software engineer", 100.054054054054),
        ("teacher", 73.2739420935412),
        ("ELSE", 58.3248114041933),
    ]

    schema = StructType(
        [StructField("jobTitle", StringType()), StructField("jobScore", FloatType())]
    )

    job_df = spark.createDataFrame(data=job_data, schema=schema)

    emp_data = [
        ("EMPLOYED", 67.9846989789796),
        ("RETIRED", 51.5473614186953),
        ("SELF_EMPLOYED", 73.7181472348072),
        ("STUDENT", 25.3895135614395),
        ("UNEMPLOYED", 32.8599412340842),
        ("ELSE", 60.6408145366188),
    ]

    schema = StructType(
        [
            StructField("employmentStatus", StringType()),
            StructField("empScore", FloatType()),
        ]
    )

    emp_df = spark.createDataFrame(data=emp_data, schema=schema)

    age_data = [
        ("b. 18-29", 39.3599307856804),
        ("c. 30-39", 65.7268727398932),
        ("d. 40-49", 72.3119890260631),
        ("e. 50-59", 73.7893882441612),
        ("f. 60+", 59.130391207029),
        ("ELSE", 60.6408145366188),
    ]

    schema = StructType(
        [StructField("ageBand", StringType()), StructField("ageScore", FloatType())]
    )

    age_df = spark.createDataFrame(data=age_data, schema=schema)

    res_data_for_model = (
        res_data.join(city_df, res_data.currentCity == city_df.currentCity, "left")
        .join(cvs_df, res_data.creditScoreBand == cvs_df.creditBand, "left")
        .join(job_df, res_data.jobTitle == job_df.jobTitle, "left")
        .join(emp_df, res_data.employmentStatus == emp_df.employmentStatus, "left")
        .join(age_df, res_data.ageBand == age_df.ageBand, "left")
    )

    res_data_for_model = res_data_for_model.withColumn(
        "hasMortgage", when(col("mtg_count") > 1, 1).otherwise(0)
    )
    res_data_for_model = res_data_for_model.withColumn(
        "alltrades", col("mtg_count") + col("rev_count") + col("ins_count")
    )
    res_data_for_model = res_data_for_model.withColumn(
        "allbalances", col("mtg_balance") + col("rev_balance") + col("ins_balance")
    )
    res_data_for_model = res_data_for_model.withColumn(
        "allpayments", col("mtg_payment") + col("rev_payment") + col("ins_payment")
    )

    res_data_for_model_fields = res_data_for_model.select(
        [
            "ins_count",
            "ins_payment",
            "ins_pastDue",
            "ins_highCredit",
            "ins_creditLimit",
            "ins_balance",
            "rev_count",
            "rev_payment",
            "rev_pastDue",
            "rev_highCredit",
            "rev_creditLimit",
            "rev_balance",
            "mtg_count",
            "mtg_payment",
            "mtg_pastDue",
            "mtg_highCredit",
            "mtg_creditLimit",
            "mtg_balance",
            "cityScore",
            "cvsScore",
            "jobScore",
            "empScore",
            "ageScore",
            "hasMortgage",
            "alltrades",
            "allbalances",
            "allpayments",
            "personalIncomeNumeric",
        ]
    )

    model_me = res_data_for_model_fields.toPandas()

# COMMAND ----------

# Export/Import pickle file

import pickle

if not start_from_pickle:
    with open(
        "/Workspace/Users/wilson.kan@neofinancial.com/IncomeValidation/pkls/model_dat.pkl",
        "wb",
    ) as f:  # open a text file
        pickle.dump(model_me, f)
else:
    with open(
        "/Workspace/Users/wilson.kan@neofinancial.com/IncomeValidation/pkls/model_dat.pkl",
        "rb",
    ) as f:  # Correctly opening the file in binary read mode
        model_me = pickle.load(f)

# COMMAND ----------

model_me_na_filled = model_me.fillna(0)

div_by_1000 = [
    "ins_pastDue",
    "ins_highCredit",
    "ins_creditLimit",
    "ins_balance",
    "rev_pastDue",
    "rev_highCredit",
    "rev_creditLimit",
    "rev_balance",
    "mtg_pastDue",
    "mtg_highCredit",
    "mtg_creditLimit",
    "mtg_balance",
    "allbalances",
    "allpayments",
]

for i in div_by_1000:
    model_me_na_filled[i] = model_me_na_filled[i] / 1000

X = model_me_na_filled.drop("personalIncomeNumeric", axis=1)
y = model_me_na_filled["personalIncomeNumeric"]
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression

reg = LinearRegression().fit(X, y)

r = permutation_importance(reg, X, y, n_repeats=30, random_state=586445)
for i in r.importances_mean.argsort()[::-1]:
    if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
        print(
            f"{X.columns[i]:<8} "
            f"{r.importances_mean[i]:.3f}"
            f" +/- {r.importances_std[i]:.3f}"
        )

# COMMAND ----------

no_bureau = ["empScore", "cvsScore", "jobScore", "ageScore"]
with_bureau = [
    "empScore",
    "cvsScore",
    "jobScore",
    "ageScore",
    "hasMortgage",
    "ins_count",
    "allbalances",
    "rev_highCredit",
    "rev_payment"
]

final_bureau_model = LinearRegression().fit(X[with_bureau], y)
final_no_bureau_model = LinearRegression().fit(X[no_bureau], y)

# COMMAND ----------

eqn_str = str(final_bureau_model.intercept_)
for i in range(len(final_bureau_model.coef_)):
  eqn_str = f"{eqn_str} + {final_bureau_model.coef_[i]} * {final_bureau_model.feature_names_in_[i]}"
print(f"bureau model: {eqn_str}\n")

eqn_str = str(final_no_bureau_model.intercept_)
for i in range(len(final_no_bureau_model.coef_)):

  eqn_str = f"{eqn_str} + {final_no_bureau_model.coef_[i]} * {final_no_bureau_model.feature_names_in_[i]}"
print(f"non bureau model: {eqn_str}\n")
print(f"bureau: {final_bureau_model.score(X[with_bureau], y)} \nno bureau: {final_no_bureau_model.score(X[no_bureau], y)}")

# COMMAND ----------





# COMMAND ----------


