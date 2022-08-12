from distutils.command.config import config
import os
import json
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, lit

##
spark = SparkSession \
                .builder \
                .appName("Resolution.py") \
                .getOrCreate()


with open("config.json") as jsonFile:
    configPath = json.load(jsonFile)

    RAW_CRM_PATH = configPath["RAW_CRM_PATH"]
    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]
    PROCESSED_CRM_PATH = configPath["PROCESSED_CRM_PATH"]
    PROCESSED_TLE_PATH = configPath["PROCESSED_TLE_PATH"]
    COMPOUNDS_CRM_PATH = configPath["COMPOUNDS_CRM_PATH"]
    COMPOUNDS_TLE_PATH = configPath["COMPOUNDS_TLE_PATH"]
    RESOLVED_PATH = configPath["RESOLVED_PATH"]

# Read raw csv
rawCRM = spark.read.option("header", True).option("inferSchema", True).csv(RAW_CRM_PATH)
rawTLE = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)


# Clean raw to parquet
## Select used columns only
## Strings to upper
## Create parsed name field
## Create parsed address field 
    ##   rawTLE(road, city, province, postal)
    ##   rawTLE(road, city, province, postal)

