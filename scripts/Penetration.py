import os
import json
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, lit

spark = SparkSession \
                .builder \
                .appName("Penetration.py") \
                .getOrCreate()

with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    RAW_CRM_PATH = configPath["RAW_CRM_PATH"]
    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]

crmDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_CRM_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).csv(RAW_TLE_PATH)

def getPenetration():
    """
    1. Filter for all account_type=="CUSTOMER" in CRM
    2. Get count of restaurants in TLE
    3. Divide (1) by (2)
    """

    customerCount = crmDF.filter(col("account_type").contains("CUSTOMER")).count()
    restaurantCount = (
        tleDF
        .select(
            col("tle_company_name"),
            col("tle_address"),
            col("tle_city"),
            col("tle_province"),
            col("tle_postal_code")
            )
        .distinct()
    ).count()

    print(customerCount)
    print(restaurantCount)
    print(f"{customerCount/restaurantCount}")

getPenetration()

print(crmDF.columns)
print(tleDF.columns)
crmDF.groupBy("city").count().show()
tleDF.groupBy("tle_city").count().show()