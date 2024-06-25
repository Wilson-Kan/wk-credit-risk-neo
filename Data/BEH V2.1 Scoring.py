# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select referenceDate, accountId, bad_rate, modelScore, modelVersion from neo_data_science_production.credit_risk_behavior_pd_v2_1 where referenceDate >= '2024-01-01'

# COMMAND ----------

def risk_flag(df):
    
    if (df['bad_rate'] <= 0.058):
        return '6 - Lowest risk'
    elif (df['bad_rate'] <= 0.186):
        return '5 - Low risk'
    elif (df['bad_rate'] <= 0.275):
        return '4 - Medium low risk'
    elif (df['bad_rate'] <= 0.391):
        return '3 - Medium high risk'
    elif (df['bad_rate'] <= 0.439):
        return '2 - High risk'
    else:
        return '1 - Highest risk'
      
p_out=_sqldf.toPandas()
p_out['risk_flag'] = p_out.apply(risk_flag, axis = 1)
p_out.sort_values('risk_flag')


# COMMAND ----------

#write to delta lake
df_sp = spark.createDataFrame(p_out)
df_sp.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('neo_views_credit_risk.beh_v21_2024_01_06_segmented_v2')

# COMMAND ----------

p_out
