import requests
import os
import sys
import time
import json
import pandas as pd
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials


os.environ['TZ'] = 'UTC';
todays_date      = date.today()+timedelta(-3)
seven_day_back   = todays_date +timedelta(-28);
end_dates        = todays_date 


start_date  = seven_day_back.strftime("%Y-%m-%d");
end_date    =  end_dates.strftime("%Y-%m-%d")

start_date = start_date +" 00:00:00";
end_date   = end_date   +" 00:00:00";

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date   = f"'{end_date}'"

def upload_gsheet(excel_file,sheet_name,header_list,overall_snapshot):

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

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

def National_Metric():

        # Header details for POST request
        headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
        }
        # Number of devices who reached on onboarding
        query_str = '''SELECT "dimensions_pdata_id"  AS "Platform",COUNT(DISTINCT "dimensions_did") AS "Unique_Device",COUNT("dimensions_did") AS "Play",SUM("total_time_spent") AS "Total_Time_Seconds" FROM "druid"."summary-events" WHERE  "dimensions_pdata_id" IN('prod.diksha.app','prod.diksha.portal') AND "dimensions_mode" = 'play' AND "dimensions_type" = 'content' AND "object_rollup_l1" = 'do_31290608850520473612338' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date+'''GROUP BY "dimensions_pdata_id" '''
        print(query_str)
        data = {"query": query_str}
        jsondata = json.dumps(data);

        # Fetching data from Druid using POST request
        try:
            response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
            x = response.json();
            if(len(x)!=0):
                df_pandas = pd.DataFrame(x);
            else:

                df_pandas = pd.DataFrame({'Platform':[None],'Unique_Device': [0], 'Play': [0], 'Total_Time_Seconds': [0]});

            df_pandas['Date'] = todays_date.strftime("%Y-%m-%d");
            print(df_pandas)
            df_slice = df_pandas[['Date', 'Platform', 'Unique_Device', 'Play', 'Total_Time_Seconds']];
            header_list = list(df_slice.columns);
            overall_snapshot = df_slice.values.tolist();
            upload_gsheet("National_Chandigarh_CCTWP_Analysis_Final","National_Rolling_Analysis",header_list,overall_snapshot)



        except Exception as e:
            print(e);


def Chandigarh_Metric():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT "dimensions_pdata_id"  AS "Platform",COUNT(DISTINCT "dimensions_did") AS "Unique_Device",COUNT("dimensions_did") AS "Play",SUM("total_time_spent") AS "Total_Time_Seconds" FROM "druid"."summary-events" WHERE  "dimensions_pdata_id" IN('prod.diksha.app','prod.diksha.portal') AND "dimensions_mode" = 'play' AND "dimensions_type" = 'content' AND "object_rollup_l1" = 'do_31290608850520473612338' AND "derived_loc_state" = 'Chandigarh' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date + '''GROUP BY "dimensions_pdata_id" '''
    print(query_str)
    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            df_pandas = pd.DataFrame(x);
        else:

            df_pandas = pd.DataFrame(
                {'Platform': [None], 'Unique_Device': [0], 'Play': [0], 'Total_Time_Seconds': [0]});

        df_pandas['Date'] = todays_date.strftime("%Y-%m-%d");
        df_slice = df_pandas[['Date', 'Platform', 'Unique_Device', 'Play', 'Total_Time_Seconds']];
        header_list = list(df_slice.columns);
        overall_snapshot = df_slice.values.tolist();
        upload_gsheet("National_Chandigarh_CCTWP_Analysis_Final", "Chandigarh_Rolling_Analysis", header_list, overall_snapshot)



    except Exception as e:
        print(e);


National_Metric();
Chandigarh_Metric();



