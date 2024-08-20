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
    st.markdown("[Source Code](https://github.com/timeplus-io/streamlit_apps/blob/main/pages/203_%F0%9F%92%BB_github_dashboard.py)", unsafe_allow_html=True)

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

def show_table_for_query(sql,table_name,row_cnt):
    st.code(sql, language="sql")
    query = Query(env=env).sql(query=sql).create()
    col = [h["name"] for h in query.metadata()["result"]["header"]]
    def update_table(row,name):
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
        if name not in st.session_state:
            st.session_state[name] = st.table(df)
        else:
            st.session_state[name].add_rows(df)
    # iterate query result
    limit = row_cnt
    count = 0
    for event in query.result():
        if event.event != "metrics" and event.event != "query":
            for row in json.loads(event.data):
                update_table(row,table_name)
                count += 1
                if count >= limit:
                    break
            # break the outer loop too
            if count >= limit:
                break
    query.cancel()
    query.delete()

col1, col2, col3 = st.columns([3,3,1])

with col1:
    st.header('New events every minute')
    sql="""SELECT window_end AS time,count() AS count from tumble(table(github_events),1m)
WHERE _tp_time > date_sub(now(), 2h) GROUP BY window_end"""
    st.code(sql, language="sql")
    result=batchQuery(sql)
    col = [h["name"] for h in result[0]]
    df = pd.DataFrame(result[1], columns=col)
    c = alt.Chart(df).mark_line(point=alt.OverlayMarkDef()).encode(x='time:T',y='count:Q',tooltip=['count'],color=alt.value('#D53C97'))
    st.altair_chart(c, use_container_width=True)

    st.header('Recent events')
    show_table_for_query('SELECT created_at,actor,type,repo FROM github_events','live_events',3)

with col2:
    #st.header('New repos')
    #show_table_for_query("""SELECT created_at,actor,repo,json_extract_string(payload,'master_branch') AS branch \nFROM github_events WHERE type='CreateEvent'""",'new_repo',3)

    st.header('Default branch for new repos')
    sql="""SELECT payload:master_branch AS branch,count(*) AS cnt
FROM table(github_events) WHERE _tp_time>date_sub(now(),1h) AND type='CreateEvent'
GROUP BY branch ORDER BY cnt DESC LIMIT 3"""
    st.code(sql, language="sql")
    result=batchQuery(sql)
    col = [h["name"] for h in result[0]]
    df = pd.DataFrame(result[1], columns=col)
    base = alt.Chart(df).encode(theta=alt.Theta('cnt:Q',stack=True),color=alt.Color('branch:N',legend=None),tooltip=['branch','cnt'])
    pie=base.mark_arc(outerRadius=80)
    text = base.mark_text(radius=110, size=16).encode(text='branch:N')
    st.altair_chart(pie+text, use_container_width=True)

    st.header('Hot repos')
    sql="""SELECT max(created_at) AS followed_at, repo, count(distinct actor) AS new_followers
FROM table(github_events) WHERE _tp_time>date_sub(now(),10m) AND type ='WatchEvent'
GROUP BY repo ORDER BY new_followers DESC LIMIT 3
"""
    show_table_for_query(sql,'star_table',3)

# show a changing single value for total events
with col3:
    st.header('Event count')
    st.code("SELECT count(*) FROM github_events EMIT periodic 1s", language="sql")
    with st.empty():
        #show the initial events first
        sql="select count(*) from table(github_events)"
        cnt=batchQuery(sql)[1][0][0]
        st.metric(label="Github events", value="{:,}".format(cnt))
        st.session_state.last_cnt=cnt

        #create a streaming query to update counts
        sql=f"select {cnt}+count(*) as events from github_events emit periodic 1s"
        query = Query(env=env).sql(query=sql).create()
        def update_row(row):
            delta=row[0]-st.session_state.last_cnt
            if (delta>0):
                st.metric(label="Github events", value="{:,}".format(row[0]), delta=row[0]-st.session_state.last_cnt, delta_color='inverse')
                st.session_state.last_cnt=row[0]
        # iterate query result
        limit = 200
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
