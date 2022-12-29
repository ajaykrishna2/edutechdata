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


def Unique_Users_Clicking_Theme_Opt_In():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Clicking_Theme_Opt_In" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_pageid"='new-experience-popup' AND "edata_id"='switch-clicked' AND "edata_subtype" ='opted-in' AND "edata_type"='new-experience' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Users_Clicking_Theme_Opt_In':x[0]['Unique_Users_Clicking_Theme_Opt_In']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Clicking_Theme_Opt_In': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;
    except Exception as e:
        print(e);

def Unique_User_Swithch_Menu_Opt_In():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_User_Swithch_Menu_Opt_In" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_pageid"='menu' AND "edata_id"='switch-clicked' AND "edata_subtype" ='opted-in' AND "edata_type"='new-experience' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],
                       'Unique_User_Swithch_Menu_Opt_In': x[0]['Unique_User_Swithch_Menu_Opt_In']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_User_Swithch_Menu_Opt_In': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);

def Unique_User_Swithch_Menu_Opt_Out():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_User_Swithch_Menu_Opt_Out" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_pageid"='menu' AND "edata_id"='switch-clicked' AND "edata_subtype" ='opted-out' AND "edata_type"='new-experience' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],
                       'Unique_User_Swithch_Menu_Opt_Out': x[0]['Unique_User_Swithch_Menu_Opt_Out']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_User_Swithch_Menu_Opt_Out': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);

def Unique_User_Clicking_Cancel_Theme_Switch():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_User_Clicking_Cancel_Theme_Switch" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_pageid"='new-experience-popup'  AND "edata_subtype" ='cancel-clicked' AND "edata_type"='new-experience' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],
                       'Unique_User_Clicking_Cancel_Theme_Switch': x[0]['Unique_User_Clicking_Cancel_Theme_Switch']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_User_Clicking_Cancel_Theme_Switch': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);

def Unique_Banner_Click():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Banner_Click" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_pageid"='home'   AND "edata_type"='select-banner' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],
                       'Unique_Banner_Click': x[0]['Unique_Banner_Click']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Banner_Click': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);

df1 = Unique_Banner_Click()
df2 = Unique_Users_Clicking_Theme_Opt_In()
df3 = Unique_User_Swithch_Menu_Opt_In()
df4 = Unique_User_Swithch_Menu_Opt_Out()
df5 = Unique_User_Clicking_Cancel_Theme_Switch()
my_dfs = [df1,df2,df3,df4,df5];

df_merge = reduce(lambda left,right: pd.merge(left,right,on='Date',how='inner'),my_dfs);
header_list  = list(df_merge.columns);
overall_snapshot = df_merge.values.tolist();
try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

            client = gspread.authorize(creds)

            # Find a workbook by name and open the first sheet
            # Make sure you use the right name here.
            sheet1 = client.open("New_Old_Experience").worksheet("PopupAuto")
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


