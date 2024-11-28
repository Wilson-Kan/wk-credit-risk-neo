from BaseModel import BaseModel
from pyspark.sql.functions import col, when

class bko_v11_app(BaseModel):
    def model_performance(self):
        monitor_df = self.scored_datapull().join(
            self.target_datapull(), on="applicationId", how="left"
        )
        monitor_df = monitor_df.withColumn(
            "is_accel_co", when(col("chargedOffReason").isNotNull(), 1).otherwise(0)
        )
        score_this = monitor_df.toPandas()
        return self.gini(score_this, "probability", "is_accel_co")

    def model_features(self):
        pass

    def scored_datapull(self):
        res = spark.sql(
            f"""
    select a.applicationId, a.score, a.probability, b.applicationDecision
    from neo_data_science_production.credit_risk_application_bko_scores as a
    inner join neo_trusted_analytics.earl_application as b
    on a.applicationId = b.applicationId
    where applicationDate between '{self.pop_start}' and '{self.pop_end}'
    and applicationDecision = 'APPROVED'
  """
        )
        return res

    def target_datapull(self):
        res = spark.sql(
            f"""
    select distinct applicationId, chargedOffReason
    from neo_trusted_analytics.earl_account
    where chargedOffAt_mt between '{self.window_start}' and '{self.window_end}'
    and chargedOffReason in   ("CONSUMER_PROPOSALS",
    "BANKRUPTCY",
    "CREDIT_COUNSELLING_SOLUTIONS",
    "SETTLEMENTS")
  """
        )
        return res