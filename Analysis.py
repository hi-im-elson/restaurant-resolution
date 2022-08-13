import os
import json
import re
import pandas as pd
import findspark
findspark.init(os.getenv("SPARK_HOME"))


from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, upper
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

# colours = {
#     "black": "1C1C1C",
#     "dark-grey": "6F7878",
#     "light-grey": "B0B0B0",
#     "yellow": "FEB32E",
#     "blue": "161B33",
# }

crmDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_CRM_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)

print(crmDF.columns)
print(tleDF.columns)

## Overall penetration
# customerCount = crmDF.filter(col("account_type").contains("CUSTOMER")).count()
# marketCount = tleDF.count()
# print(f"Loma's penetration in the market is: {customerCount}/{marketCount} ({round((customerCount/marketCount)*100, 2)}%)")

## City
cityPerformanceDF = (
    crmDF
    .select(upper("city").alias("city"), col("account_type").alias("accountType"))
    .groupBy("city")
    .pivot("accountType")
    .count()
    .join(tleDF.groupBy("tle_city").count().select(col("tle_city").alias("city"), col("count").alias("market")), ["city"], "full")
    .withColumn("customer", col("CUSTOMER"))
    .withColumn("prospect", col("PROSPECT"))
    .withColumn("currentPenetration", f.round(col("customer")/col("market"), 2))
    .withColumn("potentialPenetration", f.round((col("customer") + col("prospect"))/col("market"), 2))
    )

## To do:
# Add widget for prospect (in crm and non-crm) conversion
# Add widget for dollar value per customer
# cityPerformanceDF.show()


# ## Cuisine
crmDF.groupBy("naics_description", "sic_6_description").pivot("account_type").count().show(truncate=False)
# tleDF.groupBy("tle_market_segment", "tle_menu_type").count().show(truncate=False)
tleDF.filter(col("tle_market_segment").contains("FSR")).groupBy("tle_market_segment").count().show(50, truncate=False)
# tleDF.show(truncate=False)


# ## Tenure


# ## Size (Employee and Revenue)