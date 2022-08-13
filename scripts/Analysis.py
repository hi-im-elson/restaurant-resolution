import os
import json
import re
import pandas as pd
import findspark
findspark.init(os.getenv("SPARK_HOME"))


from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, upper, desc, when
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


# Restaurant type
# TO DO:
## Explore chains (uniforms)

typePerformanceDF = (
    crmDF.select("account_type", when(col("naics_description").contains("Full"), True).otherwise(False).alias("isFSR")).groupBy("isFSR").pivot("account_type").count().orderBy(desc("CUSTOMER"))
    .join(
        tleDF.select(when(col("tle_market_segment").contains("FSR"), True).otherwise(False).alias("isFSR")).groupBy("isFSR").count(),
        ["isFSR"], "inner"
    )
    .withColumn("customer", col("CUSTOMER"))
    .withColumn("prospect", col("PROSPECT"))
    .withColumnRenamed("count", "market")
    .withColumn("currentPenetration", f.round(col("customer")/col("market"), 2))
    .withColumn("potentialPenetration", f.round((col("customer") + col("prospect"))/col("market"), 2))
)

# typePerformanceDF.show()


## Penetration by estimated revenue
revenuePerformanceDF = (
    crmDF
    .filter(col("estimated_annual_revenue_of_business").isNotNull())
    .select("account_type",
            when(col("estimated_annual_revenue_of_business").contains("$1,000 - $499,999"),"1 - <$500K")
            .otherwise(when(col("estimated_annual_revenue_of_business").contains("$500,000 - $999,999"),"2 - <$1M")
            .otherwise(when(col("estimated_annual_revenue_of_business").contains("$1,000,000 - $2,499,999"),"3 - <$2.5M")
            .otherwise(when(col("estimated_annual_revenue_of_business").contains("$2,500,000 - $4,999,999"),"4 - <$5M")
            .otherwise("5 - >=$5M"))))
            .alias("estimatedAnnualRevenue"))
    .groupBy("estimatedAnnualRevenue")
    .pivot("account_type")
    .count()
    .join(
        tleDF
        .select(when(col("tle_annual_sales_estimate").contains("<=$500,000"),"1 - <$500K")
                .otherwise(when(col("tle_annual_sales_estimate").contains("$500,001 - $1,000,000"),"2 - <$1M")
                .otherwise(when(col("tle_annual_sales_estimate").contains("$1,000,001 - $2,500,000"),"3 - <$2.5M")
                .otherwise(when(col("tle_annual_sales_estimate").contains("$2,500,001 - $5,000,000"),"4 - <$5M")
                .otherwise("5 - >=$5M"))))
                .alias("estimatedAnnualRevenue"))
        .groupBy("estimatedAnnualRevenue")
        .count(),
        ["estimatedAnnualRevenue"], "inner"
        )
    .withColumn("customer", col("CUSTOMER"))
    .withColumn("prospect", col("PROSPECT"))
    .withColumnRenamed("count", "market")
    .withColumn("currentPenetration", f.round(col("customer")/col("market"), 2))
    .withColumn("potentialPenetration", f.round((col("customer") + col("prospect"))/col("market"), 2))
    .orderBy("estimatedAnnualRevenue")
)

# revenuePerformanceDF.show(truncate=False)

#--- Penetration by employees 
## Poor data quality for this field in TLE (5.6K rows missing)
## 

employeePerformanceDF = (
    crmDF
    .select("account_type",
            when(col("n_employees").isNull(),"0 - NULL")
            .otherwise(when(col("n_employees").contains("1 - 4 employees"),"1 - <5")
            .otherwise(when(col("n_employees").contains("5 - 9 employees"),"2 - <10")
            .otherwise(when(col("n_employees").contains("10 - 19 employees"),"3 - <20")
            .otherwise(when(col("n_employees").contains("20 - 49 employees"),"4 - <50")
            .otherwise("5 - >=50")))))
            .alias("numberOfEmployees"))
    .groupBy("numberOfEmployees")
    .pivot("account_type")
    .count()
    .join(
        tleDF
        .select(
            when(col("tle_number_employees").contains("UNCODED"),"0 - NULL")
            .otherwise(when(col("tle_number_employees").contains("1 TO 4"),"1 - <5")
            .otherwise(when(col("tle_number_employees").contains("5 TO 9"),"2 - <10")
            .otherwise(when(col("tle_number_employees").contains("10 TO 19"),"3 - <20")
            .otherwise(when(col("tle_number_employees").contains("20 TO 49"),"4 - <50")
            .otherwise("5 - >=50")))))
            .alias("numberOfEmployees"))
    .groupBy("numberOfEmployees")
        .count(),
        ["numberOfEmployees"], "inner"
        )
    .withColumn("customer", col("CUSTOMER"))
    .withColumn("prospect", col("PROSPECT"))
    .withColumnRenamed("count", "market")
    .withColumn("currentPenetration", f.round(col("customer")/col("market"), 2))
    .withColumn("potentialPenetration", f.round((col("customer") + col("prospect"))/col("market"), 2))
    .orderBy("numberOfEmployees")
)

employeePerformanceDF.show(truncate=False)