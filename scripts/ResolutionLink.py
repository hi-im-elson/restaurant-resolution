#---- Notes ----#
# Part 3 of Resolution
# This script is used to combine the compounds from both datasets to create a single view of the entity
# All compounds are used in this example but removing compounds from the LAX group would reduce the number of matches while increasing the confidence of matches
#---- Notes ----#

import os
import json
import re
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit, upper, udf, when, desc, array, explode, substring, round
from pyspark.sql.functions import regexp_replace, concat, split, element_at, trim, array_distinct, collect_set, size
from pyspark.sql.types import *

#0 Initiate spark session and read config variables
spark = SparkSession \
                .builder \
                .appName("Resolution.py") \
                .getOrCreate()

with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    COMPOUNDS_CRM_PATH = configPath["COMPOUNDS_CRM_PATH"]
    COMPOUNDS_TLE_PATH = configPath["COMPOUNDS_TLE_PATH"]
    FULL_LINKED_PATH = configPath["FULL_LINKED_PATH"]
    PROSPECTS_PATH = configPath["PROSPECTS_PATH"]

useCompoundGroup = ["STRICT", "MODERATE", "LAX"] ## Compound group to be excluded

compoundsCRM = spark.read.option("header", True).option("inferSchema", True).parquet(COMPOUNDS_CRM_PATH)
compoundsTLE = spark.read.option("header", True).option("inferSchema", True).parquet(COMPOUNDS_TLE_PATH)

## Create joins based on compounds
def explodeCompounds(df):
    return (
        df
        .select(
            col("id"),
            col("businessNameDisplay"),
            col("addressDisplay"),
            explode(col("compounds")).alias("compound")
        )
        .select(
            col("id"),
            col("businessNameDisplay"),
            col("addressDisplay"),
            col("compound.compoundName").alias("compoundName"),
            col("compound.compoundGroup").alias("compoundGroup"),
            col("compound.compoundValue").alias("compoundValue")
        )
        .filter(col("compoundValue").isNotNull())
        .filter(col("compoundGroup").isin(useCompoundGroup))
    )

explodeCRM = explodeCompounds(compoundsCRM).orderBy("businessNameDisplay", "addressDisplay")
explodeTLE = explodeCompounds(compoundsTLE).orderBy("businessNameDisplay", "addressDisplay")

linkDF = (
    explodeTLE.select([col(c).alias("tle"+c[0].upper() + c[1:]) if "compound" not in  c else col(c) for c in explodeTLE.columns])
    .join(
        explodeCRM.select([col(c).alias("crm"+c[0].upper() + c[1:]) if "compound" not in  c else col(c) for c in explodeTLE.columns]), 
        ["compoundName", "compoundGroup", "compoundValue"], 
        "inner")
    .groupBy(
        col("tleId"), 
        col("crmId"), 
        col("crmBusinessNameDisplay"), 
        col("tleBusinessNameDisplay"), 
        col("tleAddressDisplay"),
        col("crmAddressDisplay"))
    .agg(collect_set(struct("compoundName", "compoundGroup", "compoundValue")).alias("compounds"))        
    )

linkDF.write.mode("overwrite").parquet(FULL_LINKED_PATH)
print(linkDF.select("tleId").distinct().count(), linkDF.select("crmId").distinct().count())


