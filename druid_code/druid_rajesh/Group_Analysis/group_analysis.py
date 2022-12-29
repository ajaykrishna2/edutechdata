#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Tibil Solutions"
__version__ = "1.0.0"
__start__ = "2020-08-24"
__end__ = "NA"
__status__ = "Production"


import requests
import os
import sys
import json
import pandas as pd
from functools import reduce
from oauth2client.service_account import ServiceAccountCredentials
import gspread


date = sys.argv[1]
start_date = sys.argv[2];
end_date = sys.argv[3];

os.environ['TZ'] = 'UTC';

#Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def Unique_Devices_App():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_App" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_pid" = 'sunbird.app' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Device_App':x[0]['Unique_Device_App']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Device_App': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);

def Unique_Devices_Launched_App():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Launch_App" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_pid" = 'sunbird.app' AND "context_env" ='home' AND "eid"  = 'INTERACT' AND "edata_type" = 'OTHER' AND "edata_pageid" = 'splash' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        print(x)
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Device_Launch_App':x[0]['Unique_Device_Launch_App']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Launch_App': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);

def Unique_Devices_Clicked_My_Group():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Devices_Clicked_My_Group" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_pid" = 'sunbird.app' AND eid='INTERACT'
AND "context_env" = 'user' AND "edata_pageid" = 'profile' AND "edata_type" = 'TOUCH' AND "edata_subtype"= 'my-groups-clicked' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Devices_Clicked_My_Group':x[0]['Unique_Devices_Clicked_My_Group']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Devices_Clicked_My_Group': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);

def Unique_Devices_Clicked_Create_Group():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Devices_Clicked_Create_Group" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_pid" = 'sunbird.app' AND "eid" ='INTERACT'
AND "context_env" = 'group' AND "edata_pageid" = 'create-group' AND "edata_id"= 'create-group' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    print(query_str);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Devices_Clicked_Create_Group':x[0]['Unique_Devices_Clicked_Create_Group']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Devices_Clicked_Create_Group': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);

def Unique_Devices_Clicked_Add_Group():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Devices_Clicked_Add_Group" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_pid" = 'sunbird.app' AND eid='INTERACT'
AND "context_env" = 'group' AND "edata_pageid" = 'course-detail' AND "edata_type" = 'TOUCH' AND "edata_subtype"= 'add-to-group-clicked' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Devices_Clicked_Add_Group':x[0]['Unique_Devices_Clicked_Add_Group']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Devices_Clicked_Add_Group': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);

def Unique_Devices_Clicked_Add_Member():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Devices_Clicked_Add_Member" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "context_pdata_pid" = 'sunbird.app' AND eid='INTERACT'
AND "context_env" = 'group' AND "edata_pageid" = 'add-member' AND "edata_type" = 'TOUCH' AND "edata_subtype"= 'add-member-to-group-clicked' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Devices_Clicked_Add_Member':x[0]['Unique_Devices_Clicked_Add_Member']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Devices_Clicked_Add_Member': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);


df1 = Unique_Devices_App();
df2 = Unique_Devices_Launched_App();
df3 = Unique_Devices_Clicked_My_Group();
print(df3);
df4 = Unique_Devices_Clicked_Create_Group();
print(df4);
df5 = Unique_Devices_Clicked_Add_Group()
print(df5);
df6 = Unique_Devices_Clicked_Add_Member()
print(df6);
my_dfs = [df1,df2,df3,df4,df5,df6];

df_merge = reduce(lambda left,right: pd.merge(left,right,on='Date',how='inner'),my_dfs);
header_list  = list(df_merge.columns);
overall_snapshot = df_merge.values.tolist();
try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

            client = gspread.authorize(creds)

            # Find a workbook by name and open the first sheet
            # Make sure you use the right name here.
            sheet1 = client.open("Groups Metrics Tracking").worksheet("Funnel_v1")
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






