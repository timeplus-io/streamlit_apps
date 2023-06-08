import streamlit as st
import datetime,pytz
import pandas as pd
from PIL import Image
from timeplus import *
import json

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,10,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/pages/101_%F0%9F%90%A6_live_tweets.py)", unsafe_allow_html=True)
    
env = Environment().address(st.secrets["TIMEPLUS_HOST"]).apikey(st.secrets["TIMEPLUS_API_KEY"]).workspace(st.secrets["TIMEPLUS_TENANT"])

st.header("You can post a tweet with #StreamProcessing in Twitter web site or app. It will show up here in 15 seconds.")
MAX_ROW=10
st.session_state.rows=0
sql="""
SELECT created_at AS time,concat(author:name,' @',author:username) AS from,lang,text AS tweet,
  multi_if(multi_search_any(lower(text),['good','great','awesome','amazing','fast','powerful',':)']), 'positive',
           multi_search_any(lower(text),['bad','slow','hard']), 'negative','neutral') as sentiment
FROM tweets_sp WHERE NOT (tweet LIKE 'RT %') AND lang='en' AND _tp_time>now()-1d
"""
st.code(sql, language="sql")
with st.empty():
    query = Query(env=env).sql(query=sql).create()
    if query.metadata()["result"]["header"] is None:
        st.error(f"Nothing to show (query status: {query.status()}, query message: {query.message()})")
        query.cancel()
        query.delete()
    else:    
        col = [h["name"] for h in query.metadata()["result"]["header"]]
        def update_row(row,name):
            data = {}
            for i, f in enumerate(col):
                data[f] = row[i]
                #hack show first column as more friendly datetime diff
                if i==0 and isinstance(row[i], str):
                    data[f]=datetime.datetime.strptime(row[i], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
                    # Get current UTC datetime with timezone awareness
                    current_datetime = datetime.datetime.now(pytz.UTC)
                    minutes=divmod((current_datetime-data[f]).total_seconds(),60)
                    data[f]=f"{row[i]} ({int(minutes[0])} min {int(minutes[1])} sec ago)"

            df = pd.DataFrame([data], columns=col)
            st.session_state.rows=st.session_state.rows+1
            if name not in st.session_state or st.session_state.rows>=MAX_ROW:
                st.session_state[name] = st.table(df)
                st.session_state.rows=0
            else:
                st.session_state[name].add_rows(df)
        # iterate query result
        limit = MAX_ROW*10-1
        count = 0
        for event in query.result():
            if event.event != "metrics" and event.event != "query":
                for row in json.loads(event.data):
                    update_row(row,"tail")
                    count += 1
                    if count >= limit:
                        break
                # break the outer loop too    
                if count >= limit:
                    break
        query.cancel()
        query.delete()
