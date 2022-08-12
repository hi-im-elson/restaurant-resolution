import os
import findspark
findspark.init(os.getenv("SPARK_HOME"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, lit, desc

import plotly.express as px
import streamlit as st

spark = SparkSession \
                .builder \
                .appName("Exploration.py") \
                .getOrCreate()

CRM_PATH = "../data/crm/raw/loma_crm_data.csv"
TLE_PATH = "../data/tle/raw/tle_market_data.csv"

crmDF = spark.read.option("header", True).option("inferSchema", True).csv(CRM_PATH)
tleDF = spark.read.option("header", True).option("inferSchema", True).csv(TLE_PATH)

print(crmDF.columns)

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

customerDF = crmDF.filter(col("account_type").contains("CUSTOMER"))

def runBasicDqa(df, c):
    print(c)
    print(f"Distinct values in {c}: {df.select(col(c)).distinct().count()}")
    print(f"Null values in {c}: {df.select(col(c)).filter(col(c).isNull()).count()}")
    print(f"{df.groupBy(c).count().orderBy(desc('count')).show(30, truncate=False)}")

# CRM DQA
# runBasicDqa(customerDF, "city")
# runBasicDqa(customerDF, "postal_zip")
# runBasicDqa(customerDF, "naics_description")
# runBasicDqa(customerDF, "province_state")
# runBasicDqa(customerDF, "sic_6_description")
# runBasicDqa(customerDF, "estimated_annual_revenue_of_business")
# runBasicDqa(customerDF, "n_employees")

[runBasicDqa(crmDF, c) for c in crmDF.columns]
[runBasicDqa(tleDF, c) for c in tleDF.columns]
