
r__ = "Tibil Solutions"
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
    query_str = '''SELECT "context_pdata_id",COUNT(DISTINCT "context_did") AS "DAU" FROM "druid"."telemetry-events-syncts" WHERE
 "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date + ''' group by "context_pdata_id"'''

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            print(x)
            # my_dict = {'Date':[date],'Source':[Source],'Unique_Device_App':x[0]['DAU']};
            df_pandas = pd.DataFrame(x);
        else:
            pass
            # my_dict = {'Date': [date], 'Unique_Device_App': 0};
            # df_pandas = pd.DataFrame(my_dict)
        return  df_pandas;
    except Exception as e:
        print(e);

df1 = Unique_Devices_App()
df1.to_csv("DAU.csv")
print(df1)
