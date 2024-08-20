import streamlit as st
import pandas as pd
import altair as alt
from PIL import Image
import json

from timeplus import Query, Environment

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,8,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Real-time Insights for Github")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/pages/201_%F0%9F%92%BB_repos_to_follow.py)", unsafe_allow_html=True)

env = Environment().address(st.secrets["TIMEPLUS_HOST"]).apikey(st.secrets["TIMEPLUS_API_KEY"]).workspace(st.secrets["TIMEPLUS_TENANT"])

sql="SELECT top_k(repo,10) FROM github_events EMIT LAST 6m"
st.code(sql, language="sql")
query = Query(env=env).sql(query=sql).create()
chart_st=st.empty()
def update_row(row):
    df = pd.DataFrame(list(map(lambda f:{'repo':f[0],'events':f[1]},row[0])), columns=['repo','events'])
    with chart_st:
        st.altair_chart(alt.Chart(df).mark_bar().encode(x='events:Q',y=alt.Y('repo:N',sort='-x'),tooltip=['events','repo']), use_container_width=True)
# iterate query result
limit = 100
count = 0
for event in query.result():
    if event.event != "metrics" and event.event != "query":
        for row in json.loads(event.data):
            update_row(row)
            count += 1
            if count >= limit:
                break
        # break the outer loop too
        if count >= limit:
            break
query.cancel()
query.delete()
