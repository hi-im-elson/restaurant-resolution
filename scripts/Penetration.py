import os
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, lit

spark = SparkSession \
                .builder \
                .appName("Penetration.py") \
                .getOrCreate()

CRM_PATH = "../data/crm/raw/loma_crm_data.csv"
TLE_PATH = "../data/tle/raw/tle_market_data.csv"

crmDF = spark.read.option("header", True).option("inferSchema", True).csv(CRM_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).csv(TLE_PATH)

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