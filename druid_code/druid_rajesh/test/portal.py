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

def Unique_Device_Kabab_Menu():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Kabab_Menu" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'groups' and "edata_id"= 'kebab-menu' and "edata_type" = 'click' and "edata_pageid"='group-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Kabab_Menu': x[0]['Unique_Device_Kabab_Menu']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Kabab_Menu': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Enabled_Discussions():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Enabled_Discussions" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'local.sunbird.portal' and "eid" = 'INTERACT' and "context_env" = 'groups' and "edata_id"= 'confirm-enable-forum' and "edata_type" = 'click' and "edata_pageid"='group-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Enabled_Discussions': x[0]['Unique_Device_Enabled_Discussions']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Enabled_Discussions': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Forum_Icon():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Forum_Icon" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'local.sunbird.portal' and "eid" = 'INTERACT' and "context_env" = 'Course' and "edata_type" = 'click' and "edata_id"= 'discussion' and "edata_pageid"='course-consumption' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;
    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Forum_Icon': x[0]['Unique_Device_Forum_Icon']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Forum_Icon': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Adding_Topic_Details():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Adding_Topic_Details" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'discussion' and "edata_id"= 'submit-discussion-start-form' and "edata_type" = 'CLICK' and "edata_pageid"='discussion-start' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;
    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Adding_Topic_Details': x[0]['Unique_Device_Adding_Topic_Details']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Adding_Topic_Details': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Posted_Reply():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Posted_Reply" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'discussion' and "edata_id"= 'reply' and "edata_type" = 'CLICK' and "edata_pageid"='discussion-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Posted_Reply': x[0]['Unique_Device_Posted_Reply']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Posted_Reply': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Viewed_Topics():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Viewed_Topics" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'IMPRESSION' and "context_env" = 'discussion' and "edata_type" = 'view' and "edata_pageid"='discussion-home' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Viewed_Topics': x[0]['Unique_Device_Viewed_Topics']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Viewed_Topics': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Upvoted_Topic():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Upvoted_Topic" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'discussion' and "edata_id" = 'up-vote' and "edata_type" = 'CLICK' and "edata_pageid"='discussion-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Upvoted_Topic': x[0]['Unique_Device_Upvoted_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Upvoted_Topic': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Downvoted_Topic():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Downvoted_Topic" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'discussion' and "edata_id" = 'down-vote' and "edata_type" = 'CLICK' and "edata_pageid"='discussion-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Downvoted_Topic': x[0]['Unique_Device_Downvoted_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Downvoted_Topic': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Edited_Topic():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Edited_Topic" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'discussion' and "edata_id" = 'update' and "edata_type" = 'CLICK' and "edata_pageid"='discussion-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Edited_Topic': x[0]['Unique_Device_Edited_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Edited_Topic': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);

def Unique_Device_Deleted_Topic():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Deleted_Topic" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'discussion' and "edata_id" = 'delete-topic' and "edata_type" = 'CLICK' and "edata_pageid"='discussion-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Deleted_Topic': x[0]['Unique_Device_Deleted_Topic']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Deleted_Topic': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
def Unique_Device_Disabled_Discussions():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Disabled_Discussions" FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' and "eid" = 'INTERACT' and "context_env" = 'groups' and "edata_id" = 'confirm-disable-forum' and "edata_type" = 'CLICK' and "edata_pageid"='group-details' and "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);
    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Disabled_Discussions': x[0]['Unique_Device_Disabled_Discussions']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Disabled_Discussions': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;
    except Exception as e:
        print(e);
df1 = Unique_Device_Kabab_Menu();
df2 = Unique_Device_Enabled_Discussions();
df3 = Unique_Device_Forum_Icon();
df4 = Unique_Device_Adding_Topic_Details();
df5 = Unique_Device_Posted_Reply();
df6 = Unique_Device_Viewed_Topics();
df7 = Unique_Device_Upvoted_Topic();
df8 = Unique_Device_Downvoted_Topic();
df9 = Unique_Device_Edited_Topic();
df10 = Unique_Device_Deleted_Topic();
df11 = Unique_Device_Disabled_Discussions();

my_dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11];
df_merge = reduce(lambda left,right: pd.merge(left,right,on='Date',how='inner'),my_dfs);
df_merge.to_csv("portal.csv")
print(df_merge)






