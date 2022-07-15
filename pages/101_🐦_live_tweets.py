import streamlit as st
import datetime,pytz
from rx import operators as ops
import pandas as pd
from PIL import Image
from timeplus import *

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,10,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/live_tweets.py)", unsafe_allow_html=True)
    
env = (
    Env().schema(st.secrets["TIMEPLUS_SCHEMA"]).host(st.secrets["TIMEPLUS_HOST"]).port(st.secrets["TIMEPLUS_PORT"]).api_key(st.secrets["TIMEPLUS_API_KEY"])
)

st.header("You can post a tweet with #timeplus in Twitter web site or app. It will show up here in 15 seconds.")
MAX_ROW=10
st.session_state.rows=0
sql="""
SELECT created_at AS time,concat(author:name,' @',author:username) AS from,lang,text AS tweet,
  multi_if(multi_search_any(lower(text),['good','great','awesome','amazing','fast','powerful']), 'positive',
           multi_search_any(lower(text),['bad','slow','hard']), 'negative','neutral') as sentiment
FROM tweets_timeplus WHERE time>date_sub(now(),1h) SETTINGS seek_to='-1h'
"""
st.code(sql, language="sql")
with st.empty():
    query = Query().sql(sql).create()
    if query.header() is None:
        st.error(f"Nothing to show (query status: {query.status()}, query message: {query.message()})")
        query.cancel().delete()
    else:    
        col = [h["name"] for h in query.header()]
        def update_row(row,name):
            data = {}
            for i, f in enumerate(col):
                data[f] = row[i]
                #hack show first column as more friendly datetime diff
                if(i==0):
                    minutes=divmod((pytz.utc.localize(datetime.datetime.utcnow())-row[i]).total_seconds(),60)
                    data[f]=f"{row[i]} ({int(minutes[0])} min {int(minutes[1])} sec ago)"

            df = pd.DataFrame([data], columns=col)
            st.session_state.rows=st.session_state.rows+1
            if name not in st.session_state or st.session_state.rows>=MAX_ROW:
                st.session_state[name] = st.table(df)
                st.session_state.rows=0
            else:
                st.session_state[name].add_rows(df)
        query.get_result_stream().pipe(ops.take(MAX_ROW*10-1)).subscribe(
            on_next=lambda i: update_row(i,"tail"),
            on_error=lambda e: print(f"error {e}"),
            on_completed=lambda: query.stop(),
        )
        query.cancel().delete()
