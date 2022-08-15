import streamlit as st

st.title("Appendix")

with st.expander("[Penetration] What assumptions did you make to calculate total penetration in Toronto?"):
    st.write("""
    A quick data quality check was first run on each column for both tables to determine most common values, number of null values, etc.
    From the check, I was able to determine:
    1. Records in the CRM table were split by CUSTOMER and PROSPECT
    2. There were provinces beyond Ontario within the CRM dataset
    3. TLE was distinct when looking at name and address fields
    """)

with st.expander("[Performance] What further analysis would you perform given more time or resources?"):
    st.write("""
        1. FSA to Census Profile
        An understanding of the environment businesses choose to operate as well as their immediate customer base may shed light on which establishment are most likely to be potential customers.
        This might help prune and prioritize the list of leads to identify those which are most likely to convert and most valuable after conversion.

        2. Object Detection from Google or Yelp APIs
        User provided images of restaurants and what they serve could be used to identify those that use tablecloths in their establishment. Using packages like OpenCV or AutoML, a boolean field could be generated to identify if tablecloths are present in the image.

        3. Topic and sentiment analysis 
        Comments from Google and Yelp could be collected and parsed for words such as "tablecloth" or "cleanliness". The rating of the review or a sentiment analysis of the comment could be extracted to identify which locations are potentially experiencing issues with cleanliness.
        This could help prioritize leads and provide Loma's sales people with a conversation starter when commuinicating their value proposition.
        """)

with st.expander("[Performance] Why was cuisine not included in the analysis despite being in the prompt?"):
    st.write("""
    Cuisine was present in the TLE dataset but not the CRM. This made it difficult to draw comparisons without properly cleaned data.
    Given more time, the CRM data can be enriched with API data from Yelp or Google to gather more information about the businesses.
    """)


with st.expander("[Entity Resolution] How do you account for chains or franchises?"):
    st.write("""
        My assumption during this case was that each location made its own decision independently i.e. no centralized procurement for multiple locations. This was informed by the existence of businesses with multiple locations having both CUSTOMER and PROSPECT account types.
        If entity resolution were to be rerun to group multiple locations to a single organization, the grouping would occur at the business name level with multiple addresses collected to a single business.
        """)

with st.expander("[Entity Resolution] Why was string distance (Levenshtein, Jaro-Winkler, Hamming) not used for resolution?"):
    st.write("""
        String distances based on number of transformations between two strings can be misleading when it comes to entities with short or numbered names.
        For example, "MEC Inc." and "ECO Inc." would return a string distance of 2 despite being separate businesses.
        As the dataset contained numbered corporations as well e.g. 100890 Ontario Inc, this made the string distance approach less reliable.
        """)

with st.expander("[Entity Resolution] What compounds were used to resolve between the entities in both datasets?"):
    st.write("""
        List of compounds:
        * businessNameDisplay_lat_long (businessNameDisplay is the original field with an uppercase transformation only)
        * businessNameDisplay_road_city_postal
        * businessNameDisplay_road_city_fsa (fsa refers to the first three characters of a postal code)
        * businessNameClean_road_postal (businessNameClean is an array of various transformations to the businessNameDisplay e.g. removing stopwords, symbols)
        * businessNameClean_road_city_postal
        * businessNameClean_approximateLat_approximateLong (Approximation to a 100m radius of the original coordinate)
        * businessNameClean_road_city_fsa
        * businessNameClean_city_postal
        * businessNameHomophone_road_city_postal (businessNameHomophone takes the consonants from the cleaned business name)
        """)