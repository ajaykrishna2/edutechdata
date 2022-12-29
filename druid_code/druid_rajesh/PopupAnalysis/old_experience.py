import requests
import os
import sys
import time
import json
import pandas as pd
from functools import reduce
from oauth2client.service_account import ServiceAccountCredentials
import gspread


date = sys.argv[1]
start_date = sys.argv[2];
end_date = sys.argv[3];
# Timestamp to epoch time conversion
epoch1 = int(time.mktime(time.strptime(start_date, "%Y-%m-%d %H:%M:%S"))) * 1000
epoch2 = int(time.mktime(time.strptime(end_date, "%Y-%m-%d %H:%M:%S"))) * 1000

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def DAU():

    # List declaration to store fetched data
    date_list = []
    count_list = []
    date_list.append(date)

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid"='sunbird.app' AND 
    "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_ver" IN('3.8.776','3.8.787','3.8.789','3.9.808','3.9.824','3.9.830','3.9.836','4.0.853','4.0.873','4.0.875','4.1.887','4.1.892','4.2.910','4.2.911','4.3.934','4.4.947','4.4.948','4.4.958') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data)

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json()
        if (len(x) != 0):
            count_list = list(x[0].values())
        else:
            count_list.append(0)
        df_pandas = pd.DataFrame({'Date':date_list,sys._getframe().f_code.co_name:count_list})
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Launched():

    # List declaration to store fetched data
    date_list = []
    count_list = []
    date_list.append(date)

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND 
    "context_pdata_id"='prod.diksha.app' AND "context_env"='home' AND "eid"='INTERACT' AND "edata_type"='OTHER' AND "edata_pageid"='splash' 
    AND "context_pdata_ver" IN('3.8.776','3.8.787','3.8.789','3.9.808','3.9.824','3.9.830','3.9.836','4.0.853','4.0.873','4.0.875','4.1.887','4.1.892','4.2.910','4.2.911','4.3.934','4.4.947','4.4.948','4.4.958') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data)

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json()
        if (len(x) != 0):
            count_list = list(x[0].values())
        else:
            count_list.append(0)
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list})
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))

def New_Users():

    # List declaration to store fetched data
    date_list = []
    count_list = []
    date_list.append(date)

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND 
    "context_pdata_id"='prod.diksha.app' AND "context_env"='onboarding' AND "eid"='IMPRESSION' AND "edata_type"='view' AND 
    "edata_pageid"='onboarding-language-setting' AND "context_pdata_ver" IN('3.8.776','3.8.787','3.8.789','3.9.808','3.9.824','3.9.830','3.9.836','4.0.853','4.0.873','4.0.875','4.1.887','4.1.892','4.2.910','4.2.911','4.3.934','4.4.947','4.4.948','4.4.958') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data)

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json()
        if (len(x) != 0):
            count_list = list(x[0].values())
        else:
            count_list.append(0)
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list})
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))

def Unique_Device_Session():

    # List declaration to store fetched data
    date_list = []
    count_list = []
    date_list.append(date)

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of sessions of user
    query_str = '''SELECT COUNT(DISTINCT "context_sid") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND 
    "context_pdata_id"='prod.diksha.app' AND "context_pdata_ver" IN('3.8.776','3.8.787','3.8.789','3.9.808','3.9.824','3.9.830','3.9.836','4.0.853','4.0.873','4.0.875','4.1.887','4.1.892','4.2.910','4.2.911','4.3.934','4.4.947','4.4.948','4.4.958') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data)

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json()
        if (len(x) != 0):
            count_list = list(x[0].values())
        else:
            count_list.append(0)
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list})
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))

def Total_Device_Session():

    # List declaration to store fetched data
    date_list = []
    count_list = []
    date_list.append(date)

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of sessions of user
    query_str = '''SELECT COUNT("context_sid") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND 
    "context_pdata_id"='prod.diksha.app' AND "context_pdata_ver" IN('3.8.776','3.8.787','3.8.789','3.9.808','3.9.824','3.9.830','3.9.836','4.0.853','4.0.873','4.0.875','4.1.887','4.1.892','4.2.910','4.2.911','4.3.934','4.4.947','4.4.948','4.4.958') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data)

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json()
        if (len(x) != 0):
            count_list = list(x[0].values())
        else:
            count_list.append(0)
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list})
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))

def Unique_Landed_On_Library():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid"='sunbird.app' AND "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'IMPRESSION' AND "edata_type" = 'view' AND "context_env" = 'home' AND "edata_pageid" IN ('library','resources') AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Tapped_On_TextBook():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'library' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'  AND "object_type" IN ('TextBook','Digital Textbook') AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Tapped_On_Content():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'textbook-toc' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'   AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "actor_type" = 'User' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Tapped_On_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND  "edata_type" = 'TOUCH' AND "edata_subtype" IN('play-online','play-from-device') AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Tapped_On_TextBook():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'library' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'  AND "object_type" IN ('TextBook','Digital Textbook') AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Toatl_Tapped_On_Content():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'textbook-toc' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked' AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "actor_type" = 'User' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Tapped_On_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND  "edata_type" = 'TOUCH' AND "edata_subtype" IN('play-online','play-from-device') AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Overall_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'START'  AND "edata_type" = 'content' AND "context_env" IN('contentplayer','ContentPlayer')   AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Overall_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'START'  AND "edata_type" = 'content' AND "context_env" IN('contentplayer','ContentPlayer')   AND "context_cdata_type" IN ('Tabs') AND "context_cdata_id" IN ('Library-Course') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        df_pandas = pd.DataFrame({'Date': date_list, sys._getframe().f_code.co_name: count_list});
        return df_pandas

    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))



df1 = DAU();
df2 = Launched();
df3 = New_Users();
df4 = Unique_Device_Session();
df5 = Total_Device_Session();
df6 = Unique_Landed_On_Library();
df7 = Unique_Tapped_On_TextBook();
df8 = Total_Tapped_On_TextBook();
df9 = Unique_Tapped_On_Content();
df10 = Toatl_Tapped_On_Content();
df11 = Unique_Tapped_On_Play();
df12 = Total_Tapped_On_Play();
df13 = Unique_Overall_Play();
df14 = Total_Overall_Play();

my_dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14];

df_merge = reduce(lambda left,right: pd.merge(left,right,on='Date',how='inner'),my_dfs);
print(df_merge)
header_list  = list(df_merge.columns);
overall_snapshot = df_merge.values.tolist();
try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

            client = gspread.authorize(creds)

            # Find a workbook by name and open the first sheet
            # Make sure you use the right name here.
            sheet1 = client.open("New_Old_Experience").worksheet("Old_Experience")
            length_check_one = sheet1.get_all_values();
            if (len(length_check_one) == 0):
                sheet1.insert_row(header_list, 1);
            xi = sheet1.get_all_values();
            zi = len(xi);
            for w in overall_snapshot:
                zi += 1;
                sheet1.insert_row(w, zi);
except Exception as e:
            print(e);

