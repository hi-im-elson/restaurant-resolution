from gc import collect
import os
import json
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, lit, desc, substring, collect_set

spark = SparkSession \
                .builder \
                .appName("Exploration.py") \
                .getOrCreate()

# CRM_PATH = "../data/crm/raw/loma_crm_data.csv"
# TLE_PATH = "../data/tle/raw/tle_market_data.csv"
with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    RAW_CRM_PATH = configPath["RAW_CRM_PATH"]
    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]
    PROCESSED_CRM_PATH = configPath["PROCESSED_CRM_PATH"]
    PROCESSED_TLE_PATH = configPath["PROCESSED_TLE_PATH"]
    COMPOUNDS_CRM_PATH = configPath["COMPOUNDS_CRM_PATH"]
    COMPOUNDS_TLE_PATH = configPath["COMPOUNDS_TLE_PATH"]
    FULL_LINKED_PATH = configPath["FULL_LINKED_PATH"]
    PROSPECTS_PATH = configPath["PROSPECTS_PATH"]

# crmDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_CRM_PATH)
# tleDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)


# print(crmDF.columns)

## GroupBy
    # Geography
        # 1 province
        # 7 cities
        # 1511 postal codes
    # Industry
        # 5 NAICS
        # 16 SIC
    # Tenure (not in CRM)
    # Revenue
        # 8 buckets (1 null)
    # Employee size
        # 9 buckets (1 null)

# customerDF = crmDF.filter(col("account_type").contains("CUSTOMER"))

# def runBasicDqa(df, c):
#     print(c)
#     print(f"Distinct values in {c}: {df.select(col(c)).distinct().count()}")
#     print(f"Null values in {c}: {df.select(col(c)).filter(col(c).isNull()).count()}")
#     print(f"{df.groupBy(c).count().orderBy(desc('count')).show(30, truncate=False)}")

## Basic DQA
# [runBasicDqa(crmDF, c) for c in crmDF.columns]
# [runBasicDqa(tleDF, c) for c in tleDF.columns]

## Addresses per business: 
    # Each business name may have multiple locations
    # Each location may or may not be customers
    # Assumption: Decision to proceed with Loma is made on a location basis; reality: loma decision could be made by an operations team/franchise owner
# print(crmDF.groupBy("account_name", "account_type").count().orderBy(desc("count")).show(truncate=False))

linkDF = spark.read.option("header", True).option("inferSchema", True).parquet(FULL_LINKED_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).parquet(PROCESSED_TLE_PATH)
crmDF = spark.read.option("header", True).option("inferSchema", True).parquet(PROCESSED_CRM_PATH)

## CRM customers not in TLE?
crmLINK = (
    linkDF
    .filter(col("tleId").isNotNull())
    .groupBy("crmId", "crmBusinessNameDisplay","crmAddressDisplay")
    .agg(
        collect_set("tleId").alias("tleIds"),
        collect_set("tleBusinessNameDisplay").alias("tleNames"),
        collect_set("tleAddressDisplay").alias("tleAddresses")
    )
)

tleLink = (
    linkDF
    .groupBy("tleId", "tleBusinessNameDisplay", "tleAddressDisplay")
    .agg(
        collect_set("crmId").alias("crmIds"),
        collect_set("crmBusinessNameDisplay").alias("crmNames"),
        collect_set("crmAddressDisplay").alias("crmAddresses")
        )
    )


