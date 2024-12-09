from sklearn.metrics import roc_auc_score
from pyspark.sql.functions import col, when

class BaseModel(object):
  def __init__(self, name, id, pop_start, window_start, window_end, pop_end = None):
    self.name = name
    self.id = id
    self.pop_start = pop_start
    self.window_start = window_start
    self.window_end = window_end
    self.pop_end = pop_end

  def gini(self, df, y_pred, y_target):
    y_pred_proba = df[y_pred]
    y = df[y_target]
    return 2 * (roc_auc_score(y, y_pred_proba) - 0.5)

  def __str__(self):
    return f"{self.name} {self.id}"
  
  def __repr__(self):
    return self.__str__()
  
  def model_performance(self):
    pass

  def model_features(self):
    pass

  def scored_datapull(self):
    pass

  def target_datapull(self):
    pass



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