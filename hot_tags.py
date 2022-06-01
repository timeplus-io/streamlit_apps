import streamlit as st
import datetime,pytz
from rx import operators as ops
import pandas as pd
import altair as alt
from PIL import Image
from timeplus import *

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,10,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Timeplus Real-time Insights for Twitter")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/hot_tags.py) | [Tweets for timeplus](https://share.streamlit.io/timeplus-io/github_liveview/develop/repos_to_follow.py) | [About Timeplus](https://timeplus.com)", unsafe_allow_html=True)
    
env = (
    Env().schema(st.secrets["TIMEPLUS_SCHEMA"]).host(st.secrets["TIMEPLUS_HOST"]).port(st.secrets["TIMEPLUS_PORT"])
    .token(st.secrets["TIMEPLUS_TOKEN"])
    .audience(st.secrets["TIMEPLUS_AUDIENCE"]).client_id("TIMEPLUS_CLIENT_ID").client_secret("TIMEPLUS_CLIENT_SECRET")
)

sql="""
WITH cte AS (SELECT extract(text,'.*#(\\w+) .*') AS tag FROM twitter WHERE length(tag)>0)
SELECT top_k(tag,10) FROM cte SETTINGS seek_to='-1h'
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