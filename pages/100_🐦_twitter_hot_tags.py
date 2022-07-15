import streamlit as st
import datetime,pytz
from rx import operators as ops
import pandas as pd
import altair as alt
from PIL import Image
from timeplus import *

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,1,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/hot_tags.py)", unsafe_allow_html=True)
    
env = (
    Env().schema(st.secrets["TIMEPLUS_SCHEMA"]).host(st.secrets["TIMEPLUS_HOST"]).port(st.secrets["TIMEPLUS_PORT"]).api_key(st.secrets["TIMEPLUS_API_KEY"])
)

st.header("Real-time Insights for Twitter")
sql="""
WITH cte AS (SELECT extract(text,'.*#(\\w+) .*') AS tag 
FROM twitter WHERE length(tag)>0 SETTINGS seek_to='earliest')
SELECT top_k(tag,10) FROM cte EMIT PERIODIC 1s
"""
st.code(sql, language="sql")
query = Query().sql(sql).create()
chart_st=st.empty()
def update_row(row):
    df = pd.DataFrame(list(map(lambda f:{'tag':f[0],'tweets':f[1]},row[0])), columns=['tag','tweets'])
    with chart_st:
        st.altair_chart(alt.Chart(df).mark_bar().encode(x='tweets:Q',y=alt.Y('tag:N',sort='-x'),tooltip=['tweets','tag']), use_container_width=True)
query.get_result_stream().pipe(ops.take(100)).subscribe(
    on_next=lambda i: update_row(i),
    on_error=lambda e: print(f"error {e}"),
    on_completed=lambda: query.stop(),
)
query.cancel().delete()