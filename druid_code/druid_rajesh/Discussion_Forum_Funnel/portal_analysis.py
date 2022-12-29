import requests
import os
import sys
import time
import json
import pandas as pd
from functools import reduce
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import gspread


date       = sys.argv[1]
start_date = sys.argv[2];
end_date   = sys.argv[3];
os.environ['TZ'] = 'UTC';
# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def DAU():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "DAU" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'DAU': x[0]['DAU']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'DAU': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;


    except Exception as e:
        print(e);


def Unique_Users_Clicked_On_Kabab_Menu():

    # Header details for POST request
    headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
        }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Clicked_On_Kabab_Menu" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'groups' AND "edata_id" = 'kebab-menu' AND "edata_type" = 'click' AND "edata_pageid" = 'group-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
            response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
            x = response.json();
            if (len(x) != 0):
                my_dict = {'Date': [date], 'Unique_Users_Clicked_On_Kabab_Menu': x[0]['Unique_Users_Clicked_On_Kabab_Menu']};
                df_pandas = pd.DataFrame(my_dict);
                print(df_pandas);
            else:
                my_dict = {'Date': [date], 'Unique_Users_Clicked_On_Kabab_Menu': [0]};
                df_pandas = pd.DataFrame(my_dict);
            return df_pandas;


    except Exception as e:
            print(e);

def Unique_Users_Enabled_Discussion():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Enabled_Discussion" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'groups' AND "edata_id" = 'confirm-enable-forum' AND "edata_type" = 'click' AND "edata_pageid" = 'group-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],'Unique_Users_Enabled_Discussion': x[0]['Unique_Users_Enabled_Discussion']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Enabled_Discussion':[0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);


def Unique_Users_Clicked_Forum_Icon():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Clicked_Forum_Icon" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'Course' AND "edata_id" = 'discussion' AND "edata_type" = 'click' AND "edata_pageid" = 'course-consumption' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],'Unique_Users_Clicked_Forum_Icon': x[0]['Unique_Users_Clicked_Forum_Icon']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Clicked_Forum_Icon':[0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);



def Unique_Users_Clicked_Submit_After_Adding_Topicdetails():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Clicked_Submit_After_Adding_Topicdetails" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_id" = 'submit-discussion-start-form' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-start' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Clicked_Submit_After_Adding_Topicdetails': x[0]['Unique_Users_Clicked_Submit_After_Adding_Topicdetails']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Clicked_Submit_After_Adding_Topicdetails':[0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);



def Unique_Users_Viewed_Topic():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Viewed_Topic" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'IMPRESSION' AND "context_env" = 'discussion' AND "edata_type" = 'view' AND "edata_pageid" = 'discussion-home' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Viewed_Topic': x[0]['Unique_Users_Viewed_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Viewed_Topic': [0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);


def Unique_Users_Posted_Reply():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Posted_Reply" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_id" = 'reply' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Posted_Reply': x[0]['Unique_Users_Posted_Reply']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Posted_Reply': [0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

def Unique_Users_Upvoted_Topic():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Upvoted_Topic" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_id" = 'up-vote' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Upvoted_Topic': x[0]['Unique_Users_Upvoted_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Upvoted_Topic':[0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

def Unique_Users_Downvoted_Topic():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Downvoted_Topic" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_id" = 'down-vote' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Downvoted_Topic': x[0]['Unique_Users_Downvoted_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Downvoted_Topic': [0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas
    except Exception as e:
        print(e);

def Unique_Users_Updates_Topic():

        # Header details for POST request
        headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
        }
        # Number of devices who reached on onboarding
        query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Updates_Topic" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_id" = 'update' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

        data = {"query": query_str}
        jsondata = json.dumps(data);

        # Fetching data from Druid using POST request
        try:
            response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
            x = response.json();
            if (len(x) != 0):
                my_dict = {'Date': [date], 'Unique_Users_Updates_Topic': x[0]['Unique_Users_Updates_Topic']};
                df_pandas = pd.DataFrame(my_dict);
                print(df_pandas);
            else:
                my_dict = {'Date': [date], 'Unique_Users_Updates_Topic': [0]};
                df_pandas = pd.DataFrame(my_dict);
            return df_pandas;

        except Exception as e:
            print(e);


def Unique_Users_Delete_Topic():

        # Header details for POST request
        headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
        }
        # Number of devices who reached on onboarding
        query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Delete_Topic" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_id" = 'delete-topic' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

        data = {"query": query_str}
        jsondata = json.dumps(data);

        # Fetching data from Druid using POST request
        try:
            response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
            x = response.json();
            if (len(x) != 0):
                my_dict = {'Date': [date], 'Unique_Users_Delete_Topic': x[0]['Unique_Users_Delete_Topic']};
                df_pandas = pd.DataFrame(my_dict);
                print(df_pandas);
            else:
                my_dict = {'Date': [date], 'Unique_Users_Delete_Topic':[0]};
                df_pandas = pd.DataFrame(my_dict);
            return df_pandas;

        except Exception as e:
           print(e);


def Unique_Users_Disabled_Discussion():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Disabled_Discussion" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'INTERACT' AND "context_env" = 'groups' AND "edata_id" = 'confirm-disable-forum' AND "edata_type" = 'click' AND "edata_pageid" = 'group-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date],'Unique_Users_Disabled_Discussion': x[0]['Unique_Users_Disabled_Discussion']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Disabled_Discussion':[0]};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

df1  = DAU()
df2  = Unique_Users_Clicked_On_Kabab_Menu();
df3  = Unique_Users_Enabled_Discussion();
df4  = Unique_Users_Clicked_Forum_Icon();
df5  = Unique_Users_Clicked_Submit_After_Adding_Topicdetails();
df6  = Unique_Users_Posted_Reply();
df7  = Unique_Users_Viewed_Topic();
df8  = Unique_Users_Upvoted_Topic();
df9  = Unique_Users_Downvoted_Topic();
df10  = Unique_Users_Updates_Topic();
df11 = Unique_Users_Delete_Topic();
df12 = Unique_Users_Disabled_Discussion();

li = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12];
df_merge = reduce(lambda left,right: pd.merge(left,right,on='Date',how='inner'),li);
header_list  = list(df_merge.columns);
overall_snapshot = df_merge.values.tolist();
try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

            client = gspread.authorize(creds)

            # Find a workbook by name and open the first sheet
            # Make sure you use the right name here.
            sheet1 = client.open("Discussion Forum usage metrics analysis").worksheet("Portal_Funnel")
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

