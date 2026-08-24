# Databricks notebook source
# DBTITLE 1,import the required library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,create catalg ,schema and volume
spark.sql("create catalog if not exists workspace")
spark.sql("create schema if not exists workspace.default")
spark.sql("create volume if not exists workspace.default.cricket_api_project")



# COMMAND ----------

base_path='/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,calling crciket api
API_KEY="e9fff1be-df1f-4270-bda2-c7235cf949b0"
api_url=f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response=requests.get(api_url)
response.raise_for_status()

api_data=response.json() # conveting into json
print(api_data.keys())

print(json.dumps(api_data,indent=2)[:2000])

# COMMAND ----------

# DBTITLE 1,save raw api response in the volumes
raw_file_path=f'{base_path}/current_match_raw.json'

with open (raw_file_path,'w') as file:
    json.dump(api_data,file)

print("RAW API data is save at the :",raw_file_path)


# COMMAND ----------

# DBTITLE 1,create bronze layer daafram or table
bronze_data=[{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]

# COMMAND ----------

bronze_schema=StructType([
    StructField("source_api",StringType(),True),
    StructField("raw_json",StringType(),True),
     StructField("ingestion_time",TimestampType(),True) 
])
bronze_df=spark.createDataFrame(bronze_data,bronze_schema).withColumn("ingestion_time",current_timestamp())
display(bronze_df)

# COMMAND ----------

# DBTITLE 1,save the bronze table
bronze_df.write.format('delta').mode('overwrite').saveAsTable("workspace.default.cricket_bronze_current_matches")

print("BRONZE TABLE CREATED SUCCESFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current_matches