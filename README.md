# streamlit_apps

A set of streamlit demo applications to show how to easily build real-time apps with Timeplus Python SDK. Check the live demo at https://timeplus.streamlit.app/

## Basic workflow to call Timeplus API

First, you need to sign up an account on https://timeplus.com to create a workspace and get an API key.

* Use Python 3.9 or above. Import Python SDK [timeplus](https://pypi.org/project/timeplus/)
* To prepare the Timeplus API client,  `env=Environment().address(tp_host).workspace(workspaceId).apikey(tp_apikey)`
* To run a streaming query, call `q=Query(env=env).sql(query=stream_query).create()`. The column names are available via `q.metadata()["result"]["header"]` and you get the results via

```python
rows=[]
for event in q.result():
    if event.event != "metrics" and event.event != "query":
        for row in json.loads(event.data):
            rows.append(row)
q.cancel()
q.delete()
```

Check https://pypi.org/project/timeplus/ for more details, or the rest of the sample python code in this repo.

