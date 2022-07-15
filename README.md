# streamlit_apps

A set of streamlit demo applications to show how to easily build real-time apps with Timeplus SDK. 

## Basic workflow to call Timeplus API

First, you need to sign up the beta progarm of Timeplus on https://timeplus.com to get a tenant in your preferred cloud region and get an API token.

* To prepare the Timeplus API client, call `Env().schema(tp_schema).host(tp_host).port(tp_port).api_key(tp_apikey)`
* To add data to a stream, call `Stream().name(stream_name).insert(rows)`
* To run a streaming query, call `q=Query().sql(stream_query).create()` then `q.get_result_stream().pipe(ops.take(MAX_ROW)).subscribe(..)`

Check https://pypi.org/project/timeplus/ for more details.

