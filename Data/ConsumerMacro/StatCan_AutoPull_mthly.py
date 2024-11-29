# Databricks notebook source
import requests, pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items

# COMMAND ----------

def getCubeName(pid):
  url = "https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata"
  json = [{"productId": pid}]
  req = requests.post(url, json=json).json()
  return req[0]["object"]["cubeTitleEn"]

def create_data_info(vectors):
  url = "https://www150.statcan.gc.ca/t1/wds/rest/getSeriesInfoFromVector"
  json = [{"vectorId": v} for v in vectors]
  req = requests.post(url, json=json).json()
  data_info = [{"vectorId": r["object"]["vectorId"], "SeriesName": getCubeName(r["object"]["productId"]), "SubSeriesName": r["object"]["SeriesTitleEn"], "productId": r["object"]["productId"],  "coordinate": r["object"]["coordinate"], "terminated": r["object"]["terminated"]} for r in req]
  return pd.DataFrame(data_info)
  
def range_create(s, cor_len = 10):
  n = [int(p) for p in s.split(',')]
  short_list = recur(n)
  long_list = []
  for i in short_list:
    temp = i
    while len(temp) < cor_len:
      temp.append(0)
    long_list.append('.'.join(map(str, temp)))
  return(long_list)

def recur(ls, res = [[]]):
  next_res = []
  for i in range(1, ls[0]+1):
    for r in res:
      next_res.append(r + [i])
  if len(ls) == 1:
    return next_res
  return recur(ls[1:], next_res)
  
def getSCJson(path, N = 10000):
  json_dat = pd.read_csv(path)
  res_json = []
  for pid in json_dat['pid']:
    for cor in range_create(json_dat.loc[json_dat['pid'] == pid]['range'].iloc[0]):
      res_json.append({"productId": pid, "coordinate": cor, "latestN": N})
  return res_json

# COMMAND ----------

data_df = pd.DataFrame()
info_df = pd.DataFrame()
url = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromCubePidCoordAndLatestNPeriods"
url_info = "https://www150.statcan.gc.ca/t1/wds/rest/getSeriesInfoFromVector"
json_full = getSCJson("/Workspace/Users/wilson.kan@neofinancial.com/StatsCan/statcan_json_info.csv", 10000)

while len(json_full) > 0:
  print(f"data left: {len(json_full)}")
  pull_len = min(300, len(json_full))
  json = json_full[0:300]
  json_full = json_full[300:]
  req = requests.post(url, json=json).json()
  dat_list = [r["object"] for r in req]
  try:
    info_temp = create_data_info(vectors=[v["vectorId"] for v in dat_list])
    info_df = pd.concat([info_df, info_temp], ignore_index=True)
  except:
    print(f"info err: {single_data['vectorId']}, {single_data['productId']}, {single_data['coordinate']}")
  for single_data in dat_list:
    name = single_data["vectorId"]
    try:
      data_series = (
                pd.DataFrame(single_data["vectorDataPoint"])
                .assign(refPer=lambda x: pd.to_datetime(x["refPer"]))
                .set_index("refPer")
                .rename(columns={"value": name})
                .filter([name])
            )
      data_df = pd.concat([data_df, data_series], axis=1, sort=True)
    except:
      print(f"data err: {single_data['vectorId']}, {single_data['productId']}, {single_data['coordinate']}")


# COMMAND ----------

sp = spark.createDataFrame(info_df)
sp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hive_metastore.neo_views_credit_risk.wk_economic_ind_mthly_info")

data_df.reset_index(inplace=True, drop=False)
sp = spark.createDataFrame(data_df)
sp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hive_metastore.neo_views_credit_risk.wk_economic_ind_mthly_data")
