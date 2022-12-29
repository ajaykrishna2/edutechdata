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

date = sys.argv[1]
start_date = sys.argv[2];
end_date = sys.argv[3];

os.environ['TZ'] = 'UTC';

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def DAU():
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
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS DAU FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid"='sunbird.app' AND "context_pdata_id" = 'prod.diksha.app' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'DAU': x[0]['DAU']}
            df_pandas = pd.DataFrame(my_dict)
            # count_list = list(x[0].values());
        else:
            my_dict = {'Date': [date], 'DAU': 0}
            df_pandas = pd.DataFrame(my_dict)
            # count_list.append(0);
        return df_pandas

    except Exception as e:
        print(e)


def Unique_User_Enabled_Discussion():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Enabled_Discussion" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "context_env" = 'group' AND "edata_id" = 'enable-discussions' AND "edata_type" = 'success' AND "edata_pageid" = 'group-detail' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Enabled_Discussion': x[0]['Unique_Users_Enabled_Discussion']};
            df_pandas = pd.DataFrame(my_dict);

        else:
            my_dict = {'Date': [date], 'Unique_Users_Enabled_Discussion': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);


def Unique_User_Clicked_Forumicon():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_User_Clicked_Forumicon" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'IMPRESSION' AND "context_env" = 'discussion' AND "edata_type" = 'view' AND "edata_pageid" = 'discussion-home' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_User_Clicked_Forumicon': x[0]['Unique_User_Clicked_Forumicon']};
            df_pandas = pd.DataFrame(my_dict);

        else:
            my_dict = {'Date': [date], 'Unique_User_Clicked_Forumicon': 0};
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
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Clicked_Submit_After_Adding_Topicdetails" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_subtype" = 'submit-discussion-start-form' AND "edata_type" = 'CLICK' AND "edata_pageid" = 'discussion-start' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Clicked_Submit_After_Adding_Topicdetails': x[0][
                'Unique_Users_Clicked_Submit_After_Adding_Topicdetails']};
            df_pandas = pd.DataFrame(my_dict);

        else:
            my_dict = {'Date': [date], 'Unique_Users_Clicked_Submit_After_Adding_Topicdetails': [0]};
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
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Viewed_Topic" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'IMPRESSION' AND "context_env" = 'discussion'  AND "edata_type" = 'view' AND "edata_pageid" = 'discussion-details' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Viewed_Topic': x[0]['Unique_Users_Viewed_Topic']};
            df_pandas = pd.DataFrame(my_dict);

        else:
            my_dict = {'Date': [date], 'Unique_Users_Viewed_Topic': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);


def discussion_upsert(param):
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT "edata_subtype",COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.app' AND "edata_id" = 'discussion-details' AND "edata_pageid" = 'discussion-details'  AND "eid" = 'INTERACT' AND "context_env" = 'discussion' AND "edata_type"= 'CLICK'  AND "edata_subtype" IN ('up-vote','down-vote','update','delete-topic','reply') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date + '''GROUP BY "edata_subtype" '''

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        print(x);
        if (len(x) == 0):
            df_reply_slice = pd.DataFrame();
            df_upvote_slice = pd.DataFrame();
            df_downvote_slice = pd.DataFrame();
            df_update_slice = pd.DataFrame();
            df_del_topic_slice = pd.DataFrame();
            if ((param == 'reply')):
                df_reply_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Posted_Reply': [0]});
                return df_reply_slice;
            if ((param == 'up-vote')):
                df_upvote_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Upvoted_Topic': [0]});
                return df_upvote_slice;
            if ((param == 'down-vote')):
                df_downvote_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Downvoted_Topic': [0]});
                return df_downvote_slice;
            if ((param == 'update')):
                df_update_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Update_Topic': [0]});
                return df_update_slice;
            if ((param == 'delete-topic')):
                df_del_topic_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Deleted_Topic': [0]});
                return df_del_topic_slice;

        if (len(x) != 0):
            df_pandas = pd.DataFrame(x);
            print(x);
            df_pandas['Date'] = date;
            df_reply = df_pandas.loc[df_pandas['edata_subtype'] == 'reply'];
            df_upvote = df_pandas.loc[df_pandas['edata_subtype'] == 'up-vote'];
            df_downvote = df_pandas.loc[df_pandas['edata_subtype'] == 'down-vote'];
            df_update = df_pandas.loc[df_pandas['edata_subtype'] == 'update'];
            df_del_topic = df_pandas.loc[df_pandas['edata_subtype'] == 'delete-topic'];
            df_reply_slice = pd.DataFrame();
            df_upvote_slice = pd.DataFrame();
            df_downvote_slice = pd.DataFrame();
            df_update_slice = pd.DataFrame();
            df_del_topic_slice = pd.DataFrame();

            if ((df_reply.empty) & (param == 'reply')):
                df_reply_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Posted_Reply': [0]});
                return df_reply_slice
            if (~(df_reply.empty) & (param == 'reply')):
                df_reply['Unique_Users_Posted_Reply'] = df_reply['Count'];
                df_reply_slice = df_reply[['Date', 'Unique_Users_Posted_Reply']];
                return df_reply_slice;

            if ((df_upvote.empty) & (param == 'up-vote')):
                df_upvote_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Upvoted_Topic': [0]});
                return df_upvote_slice;

            if (~(df_upvote.empty) & (param == 'up-vote')):
                df_upvote['Unique_Users_Posted_Reply'] = df_upvote['Count'];
                df_upvote_slice = df_upvote[['Date', 'Unique_Users_Posted_Reply']];
                return df_upvote_slice;

            if ((df_downvote.empty) & (param == 'down-vote')):
                df_downvote_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Downvoted_Topic': [0]});
                return df_downvote_slice;

            if (~(df_downvote.empty) & (param == 'down-vote')):
                df_downvote['Unique_Users_Downvoted_Topic'] = df_downvote['Count'];
                df_downvote_slice = df_downvote[['Date', 'Unique_Users_Downvoted_Topic']];
                return df_downvote_slice;

            if ((df_update.empty) & (param == 'update')):
                df_update_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Update_Topic': [0]});
                return df_update_slice;

            if (~(df_update.empty) & (param == 'update')):
                df_update['Unique_Users_Update_Topic'] = df_update['Count'];
                df_update_slice = df_update[['Date', 'Unique_Users_Update_Topic']];
                return df_update_slice;

            if ((df_del_topic.empty) & (param == 'delete-topic')):
                df_del_topic_slice = pd.DataFrame({'Date': [date], 'Unique_Users_Deleted_Topic': [0]});
                return df_del_topic_slice;

            if (~(df_del_topic.empty) & (param == 'delete-topic')):
                df_del_topic['Unique_Users_Deleted_Topic'] = df_del_topic['Count'];
                df_del_topic_slice = df_del_topic[['Date', 'Unique_Users_Deleted_Topic']];
                return df_del_topic_slice;


    except Exception as e:
        print(e);


def Unique_User_Disabled_Discussion():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Users_Disabled_Discussion" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.app' AND "eid" = 'INTERACT' AND "context_env" = 'group' AND "edata_id" = 'disable-discussions' AND "edata_type" = 'success' AND "edata_pageid" = 'group-detail' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Users_Disabled_Discussion': x[0]['Unique_Users_Disabled_Discussion']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Users_Disabled_Discussion': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);


df1 = DAU()
print(df1)
df2 = Unique_User_Enabled_Discussion();
print(df2);
df3 = Unique_User_Clicked_Forumicon();
print(df3);
df4 = Unique_Users_Clicked_Submit_After_Adding_Topicdetails();
print(df4);
df5 = discussion_upsert('reply');
print(df5)
df6 = Unique_Users_Viewed_Topic();
df7 = discussion_upsert('up-vote');
print(df7)
df8 = discussion_upsert('down-vote');
print(df8)
df9 = discussion_upsert('update');
print(df9)
df10 = discussion_upsert('delete-topic');
print(df10)
df11 = Unique_User_Disabled_Discussion();
print(df11);
li = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11];
df_merge = reduce(lambda left, right: pd.merge(left, right, on='Date', how='inner'), li);
header_list = list(df_merge.columns);
overall_snapshot = df_merge.values.tolist();
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        '/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    sheet1 = client.open("Discussion Forum usage metrics analysis").worksheet("App_Funnel")
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

