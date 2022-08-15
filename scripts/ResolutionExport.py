from gc import collect
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

    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]
    FULL_LINKED_PATH = configPath["FULL_LINKED_PATH"]
    PROSPECTS_EXPORT_PATH = configPath["PROSPECTS_EXPORT_PATH"]
    PROSPECTS_EXPORT_CSV = configPath["PROSPECTS_EXPORT_CSV"]
    LINK_EXPORT_CSV = configPath["LINK_EXPORT_CSV"]

linkDF = spark.read.option("header", True).option("inferSchema", True).parquet(FULL_LINKED_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)

exportDF = (
    tleDF
    .join(
        linkDF.select(regexp_replace("tleId", "TLE", "").alias("tle_id"), "crmId")
              .groupBy("tle_id")
              .agg(collect_set("crmId").alias("crmIds")), 
        ["tle_id"], "left_outer")
    .filter(col("crmIds").isNull())
    )

exportDF.show()
exportDF.write.mode("overwrite").parquet(PROSPECTS_EXPORT_PATH)
exportDF.toPandas().to_csv(PROSPECTS_EXPORT_CSV, index=False)
linkDF.toPandas().to_csv(LINK_EXPORT_CSV, index=False)