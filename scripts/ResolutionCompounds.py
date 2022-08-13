#---- Notes ----#
# Part 2 of Resolution
# This script is used to extract joining compounds from the business name and address fields to resolve between datasets
# Compounds are concatenations of various fields or parsed elements of fields used to find commonality between datasets
# Compound strength is determined by the specificty of the component elements e.g. first name + last name will be less specific than first name + last name + date of birth
#---- Notes ----#

import os
import json
import re
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit, upper, udf, when, desc, array, explode, substring, round
from pyspark.sql.functions import regexp_replace, concat, split, element_at, trim, array_distinct
from pyspark.sql.types import *

#0 Initiate spark session and read config variables
spark = SparkSession \
                .builder \
                .appName("Resolution.py") \
                .getOrCreate()

with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    PROCESSED_CRM_PATH = configPath["PROCESSED_CRM_PATH"]
    PROCESSED_TLE_PATH = configPath["PROCESSED_TLE_PATH"]
    COMPOUNDS_CRM_PATH = configPath["COMPOUNDS_CRM_PATH"]
    COMPOUNDS_TLE_PATH = configPath["COMPOUNDS_TLE_PATH"]


parsedCRM = spark.read.option("header", True).option("inferSchema", True).parquet(PROCESSED_CRM_PATH)
parsedTLE = spark.read.option("header", True).option("inferSchema", True).parquet(PROCESSED_TLE_PATH)

print(parsedCRM.count(), parsedTLE.count())

def createCompounds(df):
    return (
        df
        .select(
            col("id"),
            col("parsedBusinessName.businessNameDisplay"),
            explode(col("parsedBusinessName.businessNameClean")).alias("businessNameClean"),
            col("address.addressDisplay"),
            col("address.road"),
            col("address.city"),
            col("address.postal"),
            substring(col("address.postal"),1,3).alias("fsa"),
            col("address.lat"),
            col("address.long")
        )
        .withColumn("businessNameDisplay_lat_long", concat(col("businessNameDisplay"),lit("_"),col("lat"),lit("_"),col("long")))
        .withColumn("businessNameDisplay_road_city_postal", concat(col("businessNameDisplay"),lit("_"),col("road"),lit("_"),col("city"),lit("_"),col("postal")))
        .withColumn("businessNameDisplay_road_city_fsa", concat(col("businessNameDisplay"),lit("_"),col("road"),lit("_"),col("city"),lit("_"),col("fsa")))
        .withColumn("businessNameClean_road_postal", concat(col("businessNameClean"),lit("_"),col("road"),lit("_"),col("city"),lit("_"),col("postal")))
        .withColumn("businessNameClean_road_city_postal", concat(col("businessNameClean"),lit("_"),col("road"),lit("_"),col("city"),lit("_"),col("postal")))
        .withColumn("businessNameClean_approximateLat_approximateLong", concat(col("businessNameClean"),lit("_"),round(col("lat"),4),lit("_"),round(col("long"),4)))
        .withColumn("businessNameClean_road_city_fsa", concat(col("businessNameClean"),lit("_"),col("road"),lit("_"),col("city"),lit("_"),col("fsa")))
        .withColumn("businessNameClean_city_postal", concat(col("businessNameClean"),lit("_"),col("city"),lit("_"),col("postal")))
        .withColumn("businessNameHomophone_road_city_postal", concat(regexp_replace("businessNameClean", "[AEIOU]", ""),lit("_"),col("road"),lit("_"),col("city"),lit("_"),col("postal")))
        .select(
            col("id"),
            col("businessNameDisplay"),
            col("addressDisplay"),
            array(
                struct(lit("businessNameDisplay_lat_long").alias("compoundName"), lit("STRICT").alias("compoundGroup"), col("businessNameDisplay_lat_long").alias("compoundValue")),
                struct(lit("businessNameDisplay_road_city_postal").alias("compoundName"), lit("STRICT").alias("compoundGroup"), col("businessNameDisplay_road_city_postal").alias("compoundValue")),
                struct(lit("businessNameDisplay_road_city_fsa").alias("compoundName"), lit("MODERATE").alias("compoundGroup"), col("businessNameDisplay_road_city_fsa").alias("compoundValue")),
                struct(lit("businessNameClean_road_postal").alias("compoundName"), lit("MODERATE").alias("compoundGroup"), col("businessNameClean_road_postal").alias("compoundValue")),
                struct(lit("businessNameClean_road_city_postal").alias("compoundName"), lit("MODERATE").alias("compoundGroup"), col("businessNameClean_road_city_postal").alias("compoundValue")),
                struct(lit("businessNameClean_approximateLat_approximateLong").alias("compoundName"), lit("LAX").alias("compoundGroup"), col("businessNameClean_approximateLat_approximateLong").alias("compoundValue")),
                struct(lit("businessNameClean_road_city_fsa").alias("compoundName"), lit("LAX").alias("compoundGroup"), col("businessNameClean_road_city_fsa").alias("compoundValue")),
                struct(lit("businessNameClean_city_postal").alias("compoundName"), lit("LAX").alias("compoundGroup"), col("businessNameClean_city_postal").alias("compoundValue")),
                struct(lit("businessNameHomophone_road_city_postal").alias("compoundName"), lit("LAX").alias("compoundGroup"), col("businessNameHomophone_road_city_postal").alias("compoundValue")),
                ).alias("compounds"),
            )
    )

createCompounds(parsedCRM).write.mode("overwrite").parquet(COMPOUNDS_CRM_PATH)
createCompounds(parsedTLE).write.mode("overwrite").parquet(COMPOUNDS_TLE_PATH)
