import streamlit as st
import datetime,pytz
import pandas as pd
from PIL import Image
from timeplus import Query, Environment
import json

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,8,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Real-time Insights for Github")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/pages/200_%F0%9F%92%BB_github_liveview.py)", unsafe_allow_html=True)

env = Environment().address(st.secrets["TIMEPLUS_HOST"]).apikey(st.secrets["TIMEPLUS_API_KEY"]).workspace(st.secrets["TIMEPLUS_TENANT"])

MAX_ROW=10
st.session_state.rows=0
sql='SELECT created_at,actor,type,repo FROM github_events'
st.code(sql, language="sql")
with st.empty():
    query = Query(env=env).sql(query=sql).create()
    col = [h["name"] for h in query.metadata()["result"]["header"]]
    def update_row(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
            #hack show first column as more friendly datetime diff
            if i==0 and isinstance(row[i], str):
                data[f]=datetime.datetime.strptime(row[i], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.UTC)
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

st.write(f"Only the recent {MAX_ROW*10} rows are shown. You can refresh the page to view the latest events.")
