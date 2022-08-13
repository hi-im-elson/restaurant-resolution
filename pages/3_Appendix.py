import streamlit as st

st.title("Appendix")

st.header("1. What assumptions did you make to calculate total penetration in Toronto?")

st.write("""
A quick data quality check was first run on each column for both tables to determine most common values, number of null values, etc.
From the check, I was able to determine:
1. Records in the CRM table were split by CUSTOMER and PROSPECT
2. There were provinces beyond Ontario within the CRM dataset
3. TLE was distinct when looking at name and address fields
""")


st.header("2. Why was cuisine not included in the analysis despite being in the prompt?")
st.write("""
Cuisine was present in the TLE dataset but not the CRM. This made it difficult to draw comparisons without properly cleaned data.
Given more time, the CRM data can be enriched with API data from Yelp or Google to gather more information about the businesses
""")