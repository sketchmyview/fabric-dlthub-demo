import streamlit as st
import duckdb
import altair as alt

# Connect to your DuckDB database
con = duckdb.connect('md:?motherduck_token=<motherduck_token>', read_only=True)

# Streamlit page configuration
st.title("🦆 Breaches stats | Streamlit")

# Query for filtered data
query = """
select
	domain,
	total_breaches
from my_db.mydata.breachesanalytics
"""
df = con.execute(query).df()

# Total breaches
total_downloads = df['total_breaches'].sum()

st.metric("Total Breaches > 1 by domain", total_downloads)

# Create a bar chart using Altair
chart = alt.Chart(df).mark_bar().encode(
    x='domain:N',  # 'domain' as categorical (nominal) on the x-axis
    y='total_breaches:Q'    # 'total' as quantitative on the y-axis
).properties(
    title="Total breaches by Domain (Bar Chart)"
)

# Display the chart in the Streamlit app
st.altair_chart(chart, use_container_width=True)

# Create a line chart using Altair
chart = alt.Chart(df).mark_line().encode(
    x='domain:N',  # 'domain' as categorical (nominal) on the x-axis
    y='total_breaches:Q'    # 'total' as quantitative on the y-axis
).properties(
    title="Total breaches by Domain (Line Chart)"
)

# Display the chart in the Streamlit app
st.altair_chart(chart, use_container_width=True)