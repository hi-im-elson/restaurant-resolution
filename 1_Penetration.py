import os
import json
import re
from typing import Container
import pandas as pd
import streamlit as st
st.set_page_config(layout="wide")

with open("scripts/config.json") as jsonFile:
    configPath = json.load(jsonFile)

    RAW_CRM_PATH = configPath["RAW_CRM_PATH"]
    RAW_TLE_PATH = configPath["RAW_TLE_PATH"]
    PRESENCE_ANALYSIS_PATH = configPath["PRESENCE_ANALYSIS_PATH"]

crmDF = pd.read_csv(RAW_CRM_PATH)
tleDF = pd.read_csv(RAW_TLE_PATH)
analysisDF = pd.read_parquet(PRESENCE_ANALYSIS_PATH)
# colours = {
#     "black": "1C1C1C",
#     "dark-grey": "6F7878",
#     "light-grey": "B0B0B0",
#     "yellow": "FEB32E",
#     "blue": "161B33",
# }

# TO DO / FUTURE
## Explore chains for uniform sales
## Add census data at the FSA level
## Add widget for market pen
## Add widget for estimated revenue
## Add widget for DCF and IRR

# Structure
## Overview
## Market Presence Analysis
## Financial Analysis
## Notes, Considerations and Next Steps


st.title("Birch Hill - Loma Restaurant Linens Co.")

st.header("Introduction")
st.write("""
A genuine thank you to Natacha, Aiden and the Birch Hill team for considering me up to this point. I enjoyed diving into this problem and pushing myself to find the right tech tools to answer the questions on hand.

This "memo" is structured into three pages:

    Page 1: Response to questions 1 and 2 of the case with an interactive sidebar for further analysis.
    Page 2: A walkthrough and downloadable extract of the output of an entity resolution model to identify businesses in the TLE dataset not found in Loma's CRM data.
    Page 3: Assumptions, analysis and additional context for my decisions and findings during this case.
""")



st.header("Market Presence Analysis")
st.subheader("Q1: Penetration in the Toronto Region")

customerCount = crmDF[crmDF["account_type"].str.contains("CUSTOMER") & crmDF["province_state"].str.contains("ON")].shape[0]
marketCount = tleDF.shape[0]

st.markdown(f"""
Loma's overall market presence is currently **{round((customerCount/marketCount)*100, 2)}%**. \n
**{customerCount}** distinct rows from the CRM dataset with *account_type='CUSTOMER'* and *province_state='ON'*  \n
**{marketCount}** distinct rows from TLE dataset""")



with st.sidebar:
    with st.expander("Market Presence Parameters"):
        groupBySelections = ["city", "isFSR", "estimatedAnnualRevenue", "numberOfEmployees", "fsa"]
        groupBySelector = st.selectbox(label="Presence by:", 
                                       index=0,
                                       options=groupBySelections)
                                       
        prospectConversion = st.slider(label="Prospects converted (%):",
                                       min_value=0,
                                       max_value=100,
                                       value=50)

        nonProspectConversion = st.slider(label="Non-prospects converted (%):",
                                            min_value=0,
                                            max_value=100,
                                            value=50)

        sortBySelections = ["customer", "prospect", "market", "currentPenetration", "likelyPenetration", "potentialPenetration"]
        sortBySelector = st.selectbox(label="Sort table by:", 
                                       index=0,
                                       options=sortBySelections)


    with st.expander("Financial Analysis Paramaters"):
        st.write("Params")

# leftColumn, rightColumn = st.columns(2)
# with leftColumn:
# with rightColumn:
#     st.write("Text")

st.subheader("Q2: Performance by Breakdown")
st.caption(f"### Current selection: {groupBySelector.upper()}")

breakdownObservations = {
    "city": """ While most of Loma's customers are located in Toronto, Loma's highest performing market by penetration is in Missisauga.
    Their expansion strategy appears to be focused on acquiring customers out-of-province with more than a third of their prospects being located in Quebec and Halifax.
    However, a large untapped market exists in Scarborough which could be their third largest market assuming a 1/3 conversion rate.""",

    "isFSR": """Understandably so, most of Loma's customer base and prospects fall under some category of Full Service Restaurants (FSR). 
    An opportunity that might be present in this category is the Quick Service Restaurants where uniforms are still present. """,

    "estimatedAnnualRevenue": """Loma appears to have a strong foothold in amongst smaller operators (<\$500K) and the most successful players (\$2.5M+). Yet a significant opportunity exists for Loma in the mid-market category (\$1M - $2.5M) with over 3K TLE locations being neither a Loma customer nor prospect.""",
    "numberOfEmployees": "Data quality issues with over 50% missing values in this field prevents further analysis at this time.",
    "fsa": """No analysis has been performed at the Forward Station Area just yet. However, there is an opportunity to better understand the market and its demographics using this
    breakdown due to the availability of census data using this same key. Ideas on potential analysis in Appendix"""
}

st.markdown(breakdownObservations[groupBySelector])

breakdownDF = analysisDF.groupby(groupBySelector).agg({"customer": "sum", "prospect": "sum","market": "sum"})
breakdownDF["customer"] = breakdownDF["customer"].astype(int)
breakdownDF["prospect"] = breakdownDF["prospect"].astype(int)
breakdownDF["market"] = breakdownDF["market"].astype(int)
breakdownDF["currentPenetration"] = breakdownDF["customer"] / breakdownDF["market"]
breakdownDF["likelyPenetration"] = (breakdownDF["customer"] + (prospectConversion/100)*breakdownDF["prospect"]) / breakdownDF["market"]
breakdownDF["potentialPenetration"] = breakdownDF["likelyPenetration"] + ((nonProspectConversion/100)*(breakdownDF["market"]-breakdownDF["customer"]-breakdownDF["prospect"])/breakdownDF["market"])
breakdownDF = breakdownDF.fillna(0)
st.table(breakdownDF.sort_values(sortBySelector, ascending=False))
with st.expander("Notes:"): 
    st.caption("""
        customer: account_type="CUSTOMER" in CRM dataset \n
        market: account_type="PROSPECT" in CRM dataset \n
        market: matching criteria from TLE \n
        currentPenetration = customer / market \n
        likelyPenetration = (customer + prospectConversion * prospect) / market \n
        potentialPenetration = (customer + prospectConversion * prospect + nonProspectConversion * (market - customer - prospect)) / market
    """)




st.header("Financial Analysis")
st.subheader("Revenue Forecast")

st.subheader("Investment Consideration")