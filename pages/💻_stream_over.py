import streamlit as st
import time,datetime,pytz,os,json
from rx import operators as ops
import pandas as pd
import altair as alt
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
    st.markdown("[Source Code](https://github.com/timeplus-io/github_liveview/blob/develop/stream_over.py)", unsafe_allow_html=True)

env = (
    Env().schema(st.secrets["TIMEPLUS_SCHEMA"]).host(st.secrets["TIMEPLUS_HOST"]).port(st.secrets["TIMEPLUS_PORT"]).api_key(st.secrets["TIMEPLUS_API_KEY"])
)

st.header('Event count: today vs yesterday')
sql_yesterday="""WITH cte AS( SELECT group_array(time) AS timeArray, moving_sum(cnt) AS cntArray FROM 
      (SELECT date_add(window_end,1d) AS time,count(*) AS cnt FROM tumble(table(github_events),10s) WHERE _tp_time BETWEEN date_sub(now(),1446m) AND date_sub(now(),1436m) GROUP BY window_end ORDER BY time))
SELECT t.1 AS time, t.2 AS cnt FROM (SELECT array_join(array_zip(timeArray,cntArray)) AS t FROM cte)
"""
st.markdown('<font color=#D53C97>purple line: yesterday</font>',unsafe_allow_html=True)
st.code(sql_yesterday, language="sql")
sql2="""SELECT group_array(time),moving_sum(cnt) FROM (SELECT window_end AS time,count(*) AS cnt FROM tumble(github_events,1s) GROUP BY window_end)
"""
st.markdown('<font color=#52FFDB>green line: today</font>',unsafe_allow_html=True)
st.code(sql2, language="sql")

# draw line for yesterday, 24 hours
result=Query().execSQL(sql_yesterday,100000)
col = [h["name"] for h in result["header"]]
df = pd.DataFrame(result["data"], columns=col)
chart_yesterday = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#D53C97'))

# draw half line for today
sql_today_til_now="""WITH cte AS(SELECT group_array(time) AS timeArray, moving_sum(cnt) AS cntArray FROM (SELECT window_end AS time,count(*) AS cnt FROM tumble(table(github_events),10s) WHERE _tp_time > date_sub(now(),6m) GROUP BY window_end ORDER BY time))SELECT t.1 AS time, t.2 AS cnt FROM (SELECT array_join(array_zip(timeArray,cntArray)) AS t FROM cte)"""
result=Query().execSQL(sql_today_til_now,1000000)
result_data=result["data"]
df = pd.DataFrame(result_data, columns=['time','cnt'])
chart_today_til_now = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#52FFDB'))

chart_st=st.empty()
with chart_st:
    st.altair_chart(chart_yesterday+chart_today_til_now, use_container_width=True)

# draw second half of the line for upcoming data
# cache the last count in this result
last_cnt=result_data.pop()[1]
query = Query().sql(sql2).create()
def update_row(row):
    try:
        rows=[]
        for i in range(len(row[0])):
            rows.append([row[0][i],row[1][i]+last_cnt])
        df = pd.DataFrame(rows, columns=['time','cnt'])
        chart_live = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#52FFDB'))
        with chart_st:
            st.altair_chart(chart_yesterday+chart_today_til_now+chart_live, use_container_width=True)
    except BaseException as err:
        with chart_st:
            st.error(f"Got an error while rendering chart. Please refresh the page.{err=}, {type(err)=}")
            query.cancel().delete()
query.get_result_stream().pipe(ops.take(600)).subscribe(
    on_next=lambda i: update_row(i),
    on_error=lambda e: print(f"error {e}"),
    on_completed=lambda: query.stop(),
)
query.cancel().delete()

