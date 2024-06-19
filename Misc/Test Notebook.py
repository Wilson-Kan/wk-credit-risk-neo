# Databricks notebook source
# MAGIC %md
# MAGIC ## This is a test notebook

# COMMAND ----------

#Define the trial function for simulation
import random
def trial():
  #Generate 100 random numbers between 0 and 100
  rand_list = [random.randint(0,100) for i in range(25)]

  #Select the largest of the list
  max_num = max(rand_list)

  return max_num

# COMMAND ----------

#Define the simulation
def simu(n:int):
  out_dict = dict()
  out_dict['x'] = []
  out_dict['y'] = []
  for i in range(n):
    out_dict['x'].append(i)
    out_dict['y'].append(trial())
  return out_dict

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(simu(100))

# COMMAND ----------

display(df)

# COMMAND ----------

df.hist(column = 'y')

# COMMAND ----------


