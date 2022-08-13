import json
import pandas as pd
import jellyfish
import streamlit as st
st.set_page_config(layout="wide")

with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    RAW_CRM_PATH = configPath["RAW_CRM_PATH"]
    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]
    PROCESSED_CRM_PATH = configPath["PROCESSED_CRM_PATH"]
    PROCESSED_TLE_PATH = configPath["PROCESSED_TLE_PATH"]
    COMPOUNDS_CRM_PATH = configPath["COMPOUNDS_CRM_PATH"]
    COMPOUNDS_TLE_PATH = configPath["COMPOUNDS_TLE_PATH"]
    FULL_LINKED_PATH = configPath["FULL_LINKED_PATH"]
    PROSPECTS_EXPORT_PATH = configPath["PROSPECTS_EXPORT_PATH"]


rawCRM = pd.read_csv(RAW_CRM_PATH) 
processedCRM = pd.read_parquet(PROCESSED_CRM_PATH)
compoundsCRM = pd.read_parquet(COMPOUNDS_CRM_PATH)
fullLinkedCRM = pd.read_parquet(FULL_LINKED_PATH)
tleProspectsCRM = pd.read_parquet(PROSPECTS_EXPORT_PATH)

st.header("Entity Resolution")
st.write("""
This entity resolution approach follows the following steps to identify common entities across datasets with different formatting:

    Parquetize: The ELT process begins by converting the flat csv file format into a document data model stored as a parquet
    Compound Creation: Compound keys created from multiple fields or parsed elements from fields are used to construct joining keys between datasets
    Resolution: Compounds of varying strictness joined across multiple datasets to identify common entities between them
""")

st.subheader("Parquetize")


st.write("**Raw CSV**")
st.table(rawCRM.head(3))

st.write("**Mapped Parquet**")
st.table(processedCRM.head(3))

st.subheader("Compounds")
st.write("""Compounds are created by combining elements from multiple fields together.
For example, businessName + postal will match across datasets if entities share the same name and postal code.
Another example would be businessHomophones + country which will match if entities have the same consonants in their name and reside in the same country.
The specificity of these elements is what creates a weaker or stronger compound""")

st.write("**Compounds Extracted**")
st.table(compoundsCRM.head(3))


st.subheader("Resolve")
st.write("""Compounds are linked between datasets by exploding the ArrayType column and joining on the exploded contents.
This allows multiple compounds of varying strictness/specificity to be applied across multiple datasets.
The strictness can be used as a filter to add or remove compounds to adjust for false negatives or positives""")

def getNameDistance(row):
    return jellyfish.levenshtein_distance(row["crmBusinessNameDisplay"], row["tleBusinessNameDisplay"])

def getAddressDistance(row):
    return jellyfish.levenshtein_distance(row["crmAddressDisplay"], row["tleAddressDisplay"])

st.write("**Resolved**")
sampleLinksDF = pd.concat([
    fullLinkedCRM[fullLinkedCRM["crmAddressDisplay"]!=fullLinkedCRM["tleAddressDisplay"]].tail(2),
    fullLinkedCRM[fullLinkedCRM["crmBusinessNameDisplay"]!=fullLinkedCRM["tleBusinessNameDisplay"]].head(2)
    ])
sampleLinksDF["distanceName"] = sampleLinksDF.apply(lambda x: getNameDistance(x), axis=1)
sampleLinksDF["distanceAddress"] = sampleLinksDF.apply(lambda x: getAddressDistance(x), axis=1)
sampleLinksDF = sampleLinksDF[["crmBusinessNameDisplay", "tleBusinessNameDisplay","distanceName","crmAddressDisplay","tleAddressDisplay","distanceAddress","compounds"]]

st.table(sampleLinksDF)
