import os
import json
import re
import pandas as pd
import findspark
findspark.init(os.getenv("SPARK_HOME"))


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit, upper, udf, when, desc, array, substring, concat_ws
from pyspark.sql.functions import regexp_replace, concat, split, element_at, trim, array_distinct
from pyspark.sql.types import *

#0 Initiate spark session and read config variables
spark = SparkSession \
                .builder \
                .appName("Resolution.py") \
                .getOrCreate()

with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    RAW_CRM_PATH = configPath["RAW_CRM_PATH"]
    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]
    PROCESSED_CRM_PATH = configPath["PROCESSED_CRM_PATH"]
    PROCESSED_TLE_PATH = configPath["PROCESSED_TLE_PATH"]

# colours = {
#     "black": "1C1C1C",
#     "dark-grey": "6F7878",
#     "light-grey": "B0B0B0",
#     "yellow": "FEB32E",
#     "blue": "161B33",
# }

# df = pd.read_parquet("test.parquet")

# print(newsDF.head())

# st.table(data=newsDF)

crmDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_CRM_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)

