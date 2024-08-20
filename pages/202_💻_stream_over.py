import streamlit as st
import time,datetime,pytz,os,json
import pandas as pd
import altair as alt
from PIL import Image

from timeplus import Query, Environment

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,8,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Real-time Insights for Github")
with col_link:
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/pages/202_%F0%9F%92%BB_stream_over.py)", unsafe_allow_html=True)

env = Environment().address(st.secrets["TIMEPLUS_HOST"]).apikey(st.secrets["TIMEPLUS_API_KEY"]).workspace(st.secrets["TIMEPLUS_TENANT"])

# a wrapper to run batch SQL, return headers and rows
def batchQuery(bathSQL):
    q=Query(env=env).sql(query=bathSQL).create()
    header=q.metadata()["result"]["header"]
    rows=[]
    for event in q.result():
        if event.event != "metrics" and event.event != "query":
            for row in json.loads(event.data):
                rows.append(row)
    q.cancel()
    q.delete()
    return header,rows

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
result=batchQuery(sql_yesterday)
col = [h["name"] for h in result[0]]
df = pd.DataFrame(result[1], columns=col)
chart_yesterday = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#D53C97'))

# draw half line for today
sql_today_til_now="""WITH cte AS(SELECT group_array(time) AS timeArray, moving_sum(cnt) AS cntArray FROM (SELECT window_end AS time,count(*) AS cnt FROM tumble(table(github_events),10s) WHERE _tp_time > date_sub(now(),6m) GROUP BY window_end ORDER BY time))SELECT t.1 AS time, t.2 AS cnt FROM (SELECT array_join(array_zip(timeArray,cntArray)) AS t FROM cte)"""
result=batchQuery(sql_today_til_now)
result_data=result[1]
df = pd.DataFrame(result_data, columns=['time','cnt'])
chart_today_til_now = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='cnt:Q',tooltip=['cnt',alt.Tooltip('time:T',format='%H:%M')],color=alt.value('#52FFDB'))

chart_st=st.empty()
with chart_st:
    st.altair_chart(chart_yesterday+chart_today_til_now, use_container_width=True)

# draw second half of the line for upcoming data
# cache the last count in this result
last_cnt=result_data.pop()[1]
query = Query(env=env).sql(query=sql2).create()
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

# iterate query result
limit = 600
count = 0
for event in query.result():
    if event.event != "metrics":
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
