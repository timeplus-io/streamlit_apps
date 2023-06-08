import streamlit as st
import pandas as pd
import altair as alt
from PIL import Image
from timeplus import *
import json

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,1,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/pages/100_%F0%9F%90%A6_twitter_hot_tags.py)", unsafe_allow_html=True)

env = Environment().address(st.secrets["TIMEPLUS_HOST"]).apikey(st.secrets["TIMEPLUS_API_KEY"]).workspace(st.secrets["TIMEPLUS_TENANT"])    

st.header("Real-time Insights for Twitter")
sql="""
WITH tags AS (SELECT extract(text,'.*#(\\w+) .*') AS tag 
FROM twitter WHERE _tp_time>'1970-1-1' AND length(tag)>0)
SELECT top_k(tag,10) FROM tags EMIT PERIODIC 1s
"""
st.code(sql, language="sql")
query = Query(env=env).sql(query=sql).create()
chart_st=st.empty()
def update_row(row):
    df = pd.DataFrame(list(map(lambda f:{'tag':f[0],'tweets':f[1]},row[0])), columns=['tag','tweets'])
    with chart_st:
        st.altair_chart(alt.Chart(df).mark_bar().encode(x='tweets:Q',y=alt.Y('tag:N',sort='-x'),tooltip=['tweets','tag']), use_container_width=True)
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