import logging

import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession

def main(
    spark: SparkSession,
):
    fraud_chargeoff_reasons = [
            "FIRST_PARTY",
            "SECOND_PARTY",
            "THIRD_PARTY",
            "CHARGE_BACK",
            "IDENTITY_THEFT",
            "SYNTHETIC_ID",
            "ACCOUNT_TAKE_OVER",
            "FRIENDLY_FRAUD",
            "MANIPULATION",
            "MULTIPLE_IMPRINT",
            "BUST_OUT",
            "MODIFICATION_OF_PAYMENT_ORDER",
            "POLICY_ABUSE",
            "REWARD_FRAUD",
            "PAYMENT_RISK",
            "TRANSACTION_LAUNDERING",
            "ELDER_ABUSE",
        ]

    credit_vision_characteristics = [
            "CVSC100",
            "GO14",
            "AT01",
            "AM42",
            "AM84",
            "GO148",
            "RE03",
        ]

    fraud_str = (", ").join(f"'{item}'" for item in fraud_chargeoff_reasons)

    credit_vision_str = (", ").join(
            f"cast(nullif({item},'') as integer) as {item}" for item in credit_vision_characteristics
        )

    users = spark.sql(
            """
            select
                _id as userId,
                case
                    when array_contains(roles, 'INVESTMENT') then 1
                    else 0
                end as hasInvestmentAccount,
                case 
                    when array_contains(roles, 'SAVINGS') then 1
                    else 0
                end as hasSavingsAccount,
                size(roles) as productNum
            from neo_raw_production.user_service_users
            where type not in ('TEST', 'EMPLOYEE')
            """
    )

    charge_offs = spark.sql(
        f"""
        select
        accountId as creditAccountId,
        chargedOffAt,
        last_day(chargedOffAt) as chargedOffAtMonth,
        case
            when chargedOffReason in ({fraud_str}) then 1
            else 0 
        end as isFraud,
        case
            when chargedOffReason in ('CONSUMER_PROPOSALS', 'DELINQUENCY') then 1
            else 0 
        end as isDelinquencyChargeOff
        from neo_raw_production.credit_account_service_credit_account_chargedoff_details
        """
    )

    credit_accounts = spark.sql(
        """
            select
            _id as creditAccountId, 
            userId,
            case
                when brand in ("HBC", "NEO", "SIENNA")
                then brand
                else null
            end
            brand
            from neo_raw_production.credit_account_service_credit_accounts
            """
    )
    money_balances = spark.sql(
        """
        with money_balances as (
                    select
                        userId,
                        moneyDailyTotalBalance,
                        moneyDailyBalanceDate,
                        last_day(moneyDailyBalanceDate) as lastDayOfMonth,
                        row_number() over (partition by (moneyAccountId, last_day(moneyDailyBalanceDate)) order by moneyDailyBalanceDate desc) as latestEntryPerMonth
                    from neo_trusted_production.core_money_daily_balances
        ),
        money_agg as (
            select
            userId,
            lastDayOfMonth,
            sum(moneyDailyTotalBalance) as totalMoneyBalance
            from money_balances 
            where latestEntryPerMonth = 1
            group by userId, lastDayOfMonth
        )

        select * from money_agg 
        """
)

    delinquencies = spark.sql(
        """
        with self_cure_events as (
            select distinct
                creditAccountId,
                1 as isSelfCure,
                to_date(timestamp)-1 as selfCureDate
            from neo_raw_production.credit_delinquency_service_delinquency_series_snapshots
            where resolution.reason = 'USER_PAYMENT' and invalidatedAt is null
        ),

    money_balances as (
        select
            userId,
            moneyAccountBalanceDollars,
            referenceDate
        from neo_trusted_production.core_daily_account_balance
        where moneyAccountBalanceDollars is not null
    ),

    daily_balances as (
        select 
            accountId as creditAccountId,
            userId,
            daysPastDue as delinquencyDaysPastDue,
            currentDelinquentAmountCents,
            moneyAccountBalanceDollars,
            coalesce(isSelfCure, 0) as isSelfCure,
            referenceDate,
            endOfMonthDate_ as lastDayOfMonth
        from neo_trusted_production.core_daily_account_balance as core
        left join self_cure_events
        on core.referenceDate = self_cure_events.selfCureDate and core.accountId = self_cure_events.creditAccountId
        where daysPastDue is not null and currentDelinquentAmountCents is not null  and currentDelinquentAmountCents > 0 
    ),
        
    employment_history as (
        select
            _id as userId,
            explode(employmentInfoHistory) as employment
        from neo_raw_production.user_service_users
    ),

    employment_time_frames as (
            select
                userId,
                employment.employmentStatus,
                to_timestamp(employment.updatedAt) as startTimestamp,
                coalesce(lead(to_timestamp(employment.updatedAt), 1) over(partition by userId order by to_timestamp(employment.updatedAt)), current_timestamp)as endTimestamp
            from employment_history
    ),

    feature_engineering as (
            select
                creditAccountId,
                employmentStatus,
                cast(moneyAccountBalanceDollars as integer) as moneyAccountBalanceDollars,
                delinquencyDaysPastDue,
                currentDelinquentAmountCents,
                isSelfCure,
                sum(isSelfCure) over (partition by creditAccountId order by referenceDate asc rows between unbounded preceding and 1 preceding) as cumSelfCure,
                max(isSelfCure) over (partition by creditAccountId, lastDayOfMonth) as isSelfCureMonth,
                row_number() over (partition by (creditAccountId, lastDayOfMonth) order by referenceDate desc) as latestEntryPerMonth,
                referenceDate,
                lastDayOfMonth
            from daily_balances

            left join employment_time_frames on daily_balances.userId = employment_time_frames.userId and daily_balances.referenceDate >= employment_time_frames.startTimestamp and daily_balances.referenceDate < endTimestamp
        ),

    monthly_view as (
            select
                creditAccountId,
                moneyAccountBalanceDollars,
                delinquencyDaysPastDue,
                currentDelinquentAmountCents,
                isSelfCure,
                isSelfCureMonth,
                lead(isSelfCureMonth, 1) over(partition by creditAccountId order by lastDayOfMonth) as isSelfCureNextMonth,
                coalesce(cumSelfCure, 0) as cumSelfCure,
                referenceDate,
                lastDayOfMonth,
                employmentStatus
            from feature_engineering
            where latestEntryPerMonth = 1
        )

        select * from monthly_view

        """
    )

    statements = spark.sql(
        """
            with statement_format as (
            select
                accountId as creditAccountId,
                to_date(from_utc_timestamp(closingdate,'America/Edmonton')) as closingDate,
                summary.account.previousBalanceCents as previousBalanceCents,
                summary.account.newBalanceCents as currentBalanceCents,
                summary.account.creditLimitCents,
                cast(summary.account.paymentsCents as integer) as payments
            from neo_raw_production.statement_service_credit_card_statements
            ),
            statement_select as (
            select
                creditAccountId,
                closingDate,
                coalesce(previousBalanceCents, 0) as previousBalanceCents,
                coalesce(payments, 0) as previousAmountPaidCents,
                currentBalanceCents,
                creditLimitCents,
                row_number() over(partition by creditAccountId order by closingDate asc) as statementNum,
                last_day(closingDate) as lastDayOfMonth
            from statement_format
            ),
            feature_select as (
            select
                creditAccountId,
                statementNum,
                lastDayOfMonth,
                closingDate,
                previousBalanceCents,
                currentBalanceCents,
                creditLimitCents,
                previousAmountPaidCents,
                case 
                    when previousBalanceCents > 0 and previousAmountPaidCents != 0 then previousAmountPaidCents/previousBalanceCents
                    when previousBalanceCents <=0 then 1
                    when previousAmountPaidCents =0 then 0
                end as percentOfPreviousBalancePaid,
                max(statementNum) over (partition by creditAccountId) as maxMonthsOnBook
            from statement_select
            )
            select
            creditAccountId,
            statementNum,
            lastDayOfMonth,
            currentBalanceCents,
            creditLimitCents,
            avg(percentOfPreviousBalancePaid) over (partition by creditAccountId order by lastDayOfMonth desc rows between 5 preceding and current row) as avgpercentOfPreviousBalancePaid6M,
            maxMonthsOnBook
            from feature_select
            """
    )

    credit_vision_report = spark.sql(
        f"""
        with credit_vision as (
        select
            user_id as userId,
            {credit_vision_str},
            last_day(from_utc_timestamp(createdAt, 'America/Edmonton')) lastDayOfMonth,
            row_number() over(partition by (user_id, last_day(from_utc_timestamp(createdAt, 'America/Edmonton'))) order by createdAt desc) as rowNum
        from neo_raw_production.transunion_creditreport_creditvision
        ),
        value_apply as (
        select
            userId,
            lastDayOfMonth,
            coalesce(CVSC100, last_value(CVSC100, true) over (partition by userId order by lastDayOfMonth)) as CVSC100,
            coalesce(GO14, last_value(GO14, true) over (partition by userId order by lastDayOfMonth)) as GO14,
            coalesce(AT01, last_value(AT01, true) over (partition by userId order by lastDayOfMonth)) as AT01,
            coalesce(AM42, last_value(AM42, true) over (partition by userId order by lastDayOfMonth)) as AM42,
            coalesce(AM84, last_value(AM84, true) over (partition by userId order by lastDayOfMonth)) as AM84,
            coalesce(GO148, last_value(GO148, true) over (partition by userId order by lastDayOfMonth)) as GO148,
            coalesce(RE03, last_value(RE03, true) over (partition by userId order by lastDayOfMonth)) as RE03
        from credit_vision
        where rowNum = 1
        )
        select
        userId,
        lastDayOfMonth,
        CVSC100 as creditScore,
        AM42,
        AM84,
        GO148,
        RE03,
        case
            when CVSC100 is null then 'NO_HIT_NO_SCORE'
            when CVSC100 >= 300 and CVSC100 < 640 then 'SUBPRIME'
            when CVSC100 >= 640 AND CVSC100 <= 900 then 'PRIME'
            else Null
        end as creditScoreBucket,
        case
            when GO14 > 24 and AT01 > 0 then 'THICK'
            when GO14 <= 24 or AT01 <= 0 then 'THIN'
            else Null
        end as creditFileBucket
        from value_apply
        """
    )



    design_matrix = (
    delinquencies.join(credit_accounts, "creditAccountId", "left")
    .join(users, "userId", "inner")
    .join(money_balances, ["userId", "lastDayOfMonth"], "left")
    .join(charge_offs, "creditAccountId", "left")
    .join(statements, ["creditAccountId", "lastDayOfMonth"], "left")
    .join(credit_vision_report, ["userId", "lastDayOfMonth"], "left")
    .fillna(0, ["isFraud"])
    .filter((F.col("isFraud") == 0) & (F.col("maxMonthsOnBook") > 1))
    .withColumn('lastDayOfMonth', F.date_format(F.to_date(F.col('lastDayOfMonth')), 'yyyy-MM-dd'))
    .selectExpr(
        "creditAccountId",
        "lastDayOfMonth",
        "statementNum",
        "brand",
        "creditScoreBucket",
        "creditFileBucket",
        "hasInvestmentAccount",
        "hasSavingsAccount",
        "productNum",
        "coalesce(totalMoneyBalance, 0) as totalMoneyBalance",
        "coalesce(employmentStatus, 'UNKNOWN') as employmentStatus",
        "currentBalanceCents",
        "currentDelinquentAmountCents",
        "cumSelfCure",
        "delinquencyDaysPastDue",
        "avgPercentOfPreviousBalancePaid6M",
        "creditScore",
        "AM42",
        "AM84",
        "GO148",
        "RE03",
        "isSelfCureMonth",
        "coalesce(isSelfCureNextMonth, 0) as isSelfCureNextMonth"
    )
    )
    return design_matrix