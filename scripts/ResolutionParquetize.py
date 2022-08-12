import os
import json
import re
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

#1 Read raw csv
rawCRM = spark.read.option("header", True).option("inferSchema", True).csv(RAW_CRM_PATH)
rawTLE = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)


#2 Raw to parquet

## Select columns only
mappedCRM = (
    rawCRM
    .select(
        concat(lit("CRM"), col("id")).alias("id"),
        col("account_name").alias("businessName"),
        struct(
            upper(concat_ws(" ", array(col("address"), col("city"), col("province_state"), col("postal_zip")))).alias("addressDisplay"),
            upper(col("address")).alias("road"),
            upper(col("city")).alias("city"),
            upper(col("province_state")).alias("province"),
            upper(regexp_replace("postal_zip", " ", "")).alias("postal"),
            col("latitude").alias("lat"),
            col("longitude").alias("long"),
            ).alias("address")
    )
)

mappedTLE = (
    rawTLE
    .select(
        concat(lit("TLE"), col("tle_id")).alias("id"),
        col("tle_company_name").alias("businessName"),
        struct(
            upper(concat_ws(" ", array(col("tle_address"), col("tle_city"), col("tle_province"), col("tle_postal_code")))).alias("addressDisplay"),
            upper(col("tle_address")).alias("road"),
            upper(col("tle_city")).alias("city"),
            upper(col("tle_province")).alias("province"),
            upper(regexp_replace("tle_postal_code", " ", "")).alias("postal"),
            col("tle_latitude").alias("lat"),
            col("tle_longitude").alias("long"),
            ).alias("address")
    )
)

## Create parsed name field
businessStopwordRegex = "( INC| LTD | CANADA | CO)"
foodStopwordRegex = "( RESTAURANT| BAR| CAFE| GRILL| HOUSE| KITCHEN)"

def parseBusinessName(df):
    return (
        df
        .withColumn("businessNameDisplay", upper("businessName"))
        .withColumn("removeSymbols", regexp_replace("businessNameDisplay", "[^A-Za-z0-9\\s]", ""))
        .withColumn("replaceSymbols", regexp_replace("businessNameDisplay", "[^A-Za-z0-9\\s]", " "))
        .withColumn("removeBusinessStopwords", trim(regexp_replace("removeSymbols", businessStopwordRegex, "")))
        .withColumn("removeFoodStopwords", trim(regexp_replace("removeBusinessStopwords", foodStopwordRegex, "")))
        .withColumn("parsedBusinessName", 
                    struct(
                        col("businessNameDisplay"),
                        array_distinct(array(col("removeSymbols"), 
                                             col("replaceSymbols"), 
                                             col("removeBusinessStopwords"), 
                                             col("removeFoodStopwords")
                                             )).alias("businessNameClean")))
        .drop("businessNameDisplay", "removeSymbols", "replaceSymbols", "removeBusinessStopwords", "removeFoodStopwords")
    )

parsedCRM = parseBusinessName(mappedCRM)
parsedTLE = parseBusinessName(mappedTLE)

parsedCRM.write.mode("overwrite").parquet(PROCESSED_CRM_PATH)
parsedTLE.write.mode("overwrite").parquet(PROCESSED_TLE_PATH)
