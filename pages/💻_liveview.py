import streamlit as st
import datetime,pytz
from rx import operators as ops
import pandas as pd
from PIL import Image
from timeplus import *

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,8,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Real-time Insights for Github")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/github_liveview/blob/develop/liveview.py)", unsafe_allow_html=True)
    
env = (
    Env().schema(st.secrets["TIMEPLUS_SCHEMA"]).host(st.secrets["TIMEPLUS_HOST"]).port(st.secrets["TIMEPLUS_PORT"]).api_key(st.secrets["TIMEPLUS_API_KEY"])
)

MAX_ROW=10
st.session_state.rows=0
sql='SELECT created_at,actor,type,repo FROM github_events'
st.code(sql, language="sql")
with st.empty():
    query = Query().sql(sql).create()
    col = [h["name"] for h in query.header()]
    def update_row(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
            #hack show first column as more friendly datetime diff, 2022-06-06T21:15:11
            if(i==0):
                minutes=divmod((datetime.datetime.utcnow()-datetime.datetime.strptime(row[i],"%Y-%m-%dT%H:%M:%S")).total_seconds(),60)
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

st.write(f"Only the recent {MAX_ROW*10} rows are shown. You can refresh the page to view the latest events.")