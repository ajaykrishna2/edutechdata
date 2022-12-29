import requests
import os
import sys
import time
import json
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials


date       = sys.argv[1]
start_date = sys.argv[2];
end_date   = sys.argv[3];
excel_file = "Tara usage metrics";

os.environ['TZ'] = 'UTC';

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"

def upload_gsheet(excel_file,sheet_name,df_pivot):

    try:

        header_list       = list(df_pivot.columns);
        overall_snapshot  = df_pivot.values.tolist();

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            '/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

        client = gspread.authorize(creds)

        # Find a workbook by name and open the first sheet
        # Make sure you use the right name here.
        sheet1 = client.open(excel_file).worksheet(sheet_name)
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



def Unique_Devices_Portal():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Portal" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Count':x[0]['Unique_Device_Portal']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Count': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;


    except Exception as e:
        print(e);


def Unique_Devices_Whatsapp():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "eid" = 'INTERACT' AND "edata_type" ='START' AND "context_env" = 'diksha.whatsapp' AND "edata_id" ='step1' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice          = df_pandas[['Date','Count']]
        else:
            df_slice = pd.DataFrame({'Date':[date],'Count':[0]});
        return  df_slice

    except Exception as e:
        print(e);



def Unique_Users_Clicked_On_Tara_Portal():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "eid" = 'INTERACT' AND "edata_type" ='START' AND "context_env" = 'prod.diksha.portal.bot' AND "edata_id" ='step1' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice          = df_pandas[['Date','Count']]
        else:
            df_slice = pd.DataFrame({'Date':[date],'Count':[0]});
        return  df_slice

    except Exception as e:
        print(e);


def Unique_Users_Clicked_On_Tara_Whatsapp():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "eid" = 'INTERACT' AND "edata_type" ='START' AND "context_env" = 'diksha.whatsapp' AND "edata_id" ='step1' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice          = df_pandas[['Date','Count']]
        else:
            df_slice = pd.DataFrame({'Date':[date],'Count':[0]});
        return  df_slice

    except Exception as e:
        print(e);

def column_tagging(df_pandas):

    if ((df_pandas['edata_type'] == 'CHOOSE_DIGITAL_CONTENT') & (df_pandas['edata_id'] == 'step1_1')):
        return 'Unique_Device_Clicked_On_Menu_Option_One';
    elif ((df_pandas['edata_type'] == 'TRAINING_OPTIONS') & (df_pandas['edata_id'] == 'step1_2')):
        return 'Unique_Device_Clicked_On_Menu_Option_Two'
    elif ((df_pandas['edata_type'] == 'PLAYSTORE') & (df_pandas['edata_id'] == 'step1_3')):
        return 'Unique_Device_Clicked_On_Menu_Option_Three'
    elif ((df_pandas['edata_type'] == 'CONTRIBUTE_CONTENT') & (df_pandas['edata_id'] == 'step1_4')):
        return 'Unique_Device_Clicked_On_Menu_Option_Four'
    elif ((df_pandas['edata_type'] == 'OTHER_OPTIONS') & (df_pandas['edata_id'] == 'step1_5')):
        return 'Unique_Device_Clicked_On_Menu_Option_Five'
    elif ((df_pandas['edata_type'] == 'COMIC_BOOKS') & (df_pandas['edata_id'] == 'step1_6')):
        return 'Unique_Device_Clicked_On_Menu_Option_Six'

def Menu_Funnel_Portal():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT "edata_type","edata_id",COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "context_env" = 'prod.diksha.portal.bot' AND "eid" = 'INTERACT' AND "edata_type" IN('CHOOSE_DIGITAL_CONTENT','TRAINING_OPTIONS','PLAYSTORE','QUIZ_PROGRAMS','CONTRIBUTE_CONTENT','OTHER_OPTIONS','COMIC_BOOKS') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date+'''GROUP BY "edata_id","edata_type" '''

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas = df_pandas.assign(Metric=df_pandas.apply(column_tagging,axis=1))
            df_pandas['Date'] = date
            return df_pandas
    except Exception as e:
        print(e);

def Menu_Funnel_Whatsappl():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT "edata_type","edata_id",COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "context_env" = 'diksha.whatsapp' AND "eid" = 'INTERACT' AND "edata_type" IN('CHOOSE_DIGITAL_CONTENT','TRAINING_OPTIONS','PLAYSTORE','QUIZ_PROGRAMS','CONTRIBUTE_CONTENT','OTHER_OPTIONS','COMIC_BOOKS') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date+'''GROUP BY "edata_id","edata_type" '''

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas = df_pandas.assign(Metric=df_pandas.apply(column_tagging,axis=1))
            df_pandas['Date'] = date
            return df_pandas
    except Exception as e:
        print(e);


def device_metric():

    df1 = Unique_Devices_Portal();
    df1["Source"]="Portal"
    df1['Metric']="Unique_Devices"
    df2 = Unique_Devices_Whatsapp();
    df2["Source"]="Whatsapp"
    df2["Metric"]="Unique_Devices"
    df3 = Unique_Users_Clicked_On_Tara_Portal();
    df3["Source"]="Portal"
    df3["Metric"]="Unique_Users_Clicked_On_Tara"
    df4 = Unique_Users_Clicked_On_Tara_Whatsapp()
    df4["Source"]="Whatsapp"
    df4["Metric"]="Unique_Users_Clicked_On_Tara"
    li = [df1,df2, df3,df4];
    df_union = pd.concat(li, axis=0, ignore_index=True);
    print(df_union);
    df_pivot = df_union.set_index(['Date', 'Source', 'Metric'])['Count'].unstack().reset_index();
    df_pivot.fillna(0, inplace=True);
    return df_pivot

def menu_funnel():
    df_portal = Menu_Funnel_Portal();
    df_whatsapp = Menu_Funnel_Whatsappl();

    df_portal['Source'] = 'Portal'
    if (df_whatsapp is not None):
        df_whatsapp['Source'] = 'Whatsapp'

    li = [df_whatsapp,df_portal];
    df_union = pd.concat(li, axis=0, ignore_index=True);
    print(df_union);
    df_pivot = df_union.set_index(['Date', 'Source', 'Metric'])['Count'].unstack().reset_index();
    df_pivot.fillna(0, inplace=True);
    df_pivot_slice = df_pivot[
        ['Date', 'Source', 'Unique_Device_Clicked_On_Menu_Option_One', 'Unique_Device_Clicked_On_Menu_Option_Two',
         'Unique_Device_Clicked_On_Menu_Option_Three', 'Unique_Device_Clicked_On_Menu_Option_Four',
         'Unique_Device_Clicked_On_Menu_Option_Five', 'Unique_Device_Clicked_On_Menu_Option_Six']]

    return df_pivot_slice

def column_tagging_course(df_pandas):

    if ((df_pandas['edata_type'] == 'TRAINING') & (df_pandas['edata_id'] == 'step1_2_1')):
        return 'Unique users who clicked on the menu option 2.1(Take a course)';
    elif ((df_pandas['edata_type'] == 'CHOOSE_STATE_TRAINING_BOARD') & (df_pandas['edata_id'] == 'step1_2_1_1')):
        return 'Unique users who clicked on menu option 2.1.1(State courses)'
    elif ((df_pandas['edata_type'] == 'IGOT_TRAINING') & (df_pandas['edata_id'] == 'step1_2_1_2')):
        return 'Unique users who clicked on menu option 2.1.2(IGOT courses)'
    elif ((df_pandas['edata_type'] == 'NISHTHA_TRAINING') & (df_pandas['edata_id'] == 'step1_2_1_3')):
        return 'Unique users who clicked on menu option 2.1.2(NISHTHA courses)'

def Course_Funnel_Portal():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT "edata_type","edata_id",COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "edata_subtype" = 'intent_detected' AND "context_env" = 'prod.diksha.portal.bot' AND "eid" = 'INTERACT' AND "edata_type" IN ('TRAINING','CHOOSE_STATE_TRAINING_BOARD','IGOT_TRAINING','NISHTHA_TRAINING')AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date+'''GROUP BY "edata_id","edata_type" '''

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas = df_pandas.assign(Metric=df_pandas.apply(column_tagging_course, axis=1))
            df_pandas['Date'] = date;
            return df_pandas

    except Exception as e:
        print(e);

def Course_Funnel_Whatsapp():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    query_str = '''SELECT "edata_type","edata_id",COUNT(DISTINCT "context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "edata_subtype" = 'intent_detected' AND "context_env" = 'diksha.whatsapp' AND "eid" = 'INTERACT' AND "edata_type" IN ('TRAINING','CHOOSE_STATE_TRAINING_BOARD','IGOT_TRAINING','NISHTHA_TRAINING')AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date+'''GROUP BY "edata_id","edata_type" '''

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas = df_pandas.assign(Metric=df_pandas.apply(column_tagging_course, axis=1))
            df_pandas['Date'] = date;
            return df_pandas

    except Exception as e:
        print(e);

def course_content():
    df_portal = Course_Funnel_Portal();
    df_whatsapp = Course_Funnel_Whatsapp();

    df_portal['Source'] = 'Portal'
    if (df_whatsapp is not None):
        df_whatsapp['Source'] = 'Whatsapp'


    li = [df_portal, df_whatsapp];
    df_union = pd.concat(li, axis=0, ignore_index=True);
    df_pivot = df_union.set_index(['Date', 'Source', 'Metric'])['Count'].unstack().reset_index();
    df_pivot.fillna(0, inplace=True);
    df_pivot = df_pivot.loc[df_pivot['Source'].isin(['Whatsapp', 'Portal'])];
    source_list = df_pivot['Source'].tolist();

    if ('Whatsapp' not in source_list):
        print("in if");
        df_pivot1 = pd.DataFrame({'Date': [date], 'Source': ['Whatsapp'],
                                  'Unique users who clicked on menu option 2.1.1(State courses)': [0],
                                  'Unique users who clicked on menu option 2.1.2(IGOT courses)': [0],
                                  'Unique users who clicked on menu option 2.1.2(NISHTHA courses)': [0],
                                  'Unique users who clicked on the menu option 2.1(Take a course)': [0]});
        df_pivot = df_pivot[['Date','Source','Unique users who clicked on menu option 2.1.1(State courses)','Unique users who clicked on menu option 2.1.2(IGOT courses)','Unique users who clicked on menu option 2.1.2(NISHTHA courses)','Unique users who clicked on the menu option 2.1(Take a course)']]
        df_pivot = pd.concat([df_pivot, df_pivot1], axis=0, ignore_index=True);
        df_pivot.sort_values("Source", axis=0, ascending=False, inplace=True, na_position='last')
        return df_pivot
    else:
        return df_pivot

device = device_metric();
menu   = menu_funnel();
menu_slice = menu[['Date','Source','Unique_Device_Clicked_On_Menu_Option_Two']]
course  =course_content();
df_merge = device.merge(menu_slice,on=['Date','Source'],how='outer');
df_merge.fillna(0, inplace=True);
df_merge = df_merge.merge(course,on=['Date','Source'],how='outer')
df_merge.fillna(0, inplace=True);
df_merge_slice =df_merge[['Date', 'Source',"Unique_Devices","Unique_Users_Clicked_On_Tara", 'Unique_Device_Clicked_On_Menu_Option_Two', 'Unique users who clicked on the menu option 2.1(Take a course)',
         'Unique users who clicked on menu option 2.1.1(State courses)','Unique users who clicked on menu option 2.1.2(IGOT courses)',
         'Unique users who clicked on menu option 2.1.2(NISHTHA courses)']]
df_merge_slice.sort_values("Source", axis=0, ascending=False, inplace=True, na_position='last')
upload_gsheet(excel_file,"2-Courses",df_merge_slice)



