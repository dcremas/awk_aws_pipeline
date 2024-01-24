import os
import shutil
import csv
import json
from datetime import date, timedelta
import requests
from dotenv import load_dotenv
from weatherkit.client import WKClient

load_dotenv()

days_gap = 10
date_tod = date.today().strftime("%Y-%m-%d")
date_beg = (date.today() - timedelta(days = days_gap)).strftime("%Y-%m-%d")
date_end = (date.today() + timedelta(days = days_gap)).strftime("%Y-%m-%d")
year_tod, month_tod, day_tod = date_tod[:4], date_tod[5:7], date_tod[8:]
year_beg, month_beg, day_beg = date_beg[:4], date_beg[5:7], date_beg[8:]
year_end, month_end, day_end = date_end[:4], date_end[5:7], date_end[8:]

TEAM_ID = os.getenv('AWK_TEAM_ID')
SERVICE_ID = os.getenv('AWK_SERVICE_ID')
KEY_ID = os.getenv('AWK_KEY_ID')
PEM_FILE_PATH = os.getenv('AWK_PEM_FILE_PATH')

LANGUAGE = "en"
TIMEZONE = "America/Chicago"
DATASETS = ["forecastHourly"]

client = WKClient(TEAM_ID, SERVICE_ID, KEY_ID, PEM_FILE_PATH)

headers = {"Authorization": f"Bearer {client.token.token}"}
params = {"timezone": TIMEZONE,
          "dataSets": ",".join(DATASETS),
          "currentAsOf": f'{year_tod}-{month_tod}-{day_tod}T00:00:00Z',
          "hourlyStart": f'{year_beg}-{month_beg}-{day_beg}T00:00:00Z',
          "hourlyEnd": f'{year_end}-{month_end}-{day_end}T00:00:00Z',
          "countryCode": 'US'
          }

with open("metadata/location_extract.csv", "r", newline="") as read_file:
    reader = csv.reader(read_file, delimiter="|")
    reader_data = [record for idx, record in enumerate(reader) if idx > 0]

count = 0

while (len(os.listdir("json_files_staging")) < 113) and (count < 6):

    dir_files = [file for file in os.listdir("json_files_staging")]
    staging_files = [file.split('_')[0] for file in dir_files if file.split('.')[1] == 'json']

    for item in reader_data:

        if item[0] in staging_files:
            continue
        else:
            latitude = float(item[4])
            longitude = float(item[5])

            url = f"https://weatherkit.apple.com/api/v1/weather/{LANGUAGE}/{latitude}/{longitude}"

            try:
                response = requests.get(url, headers=headers, params=params).json()
                response_str = json.dumps(response, indent=2)
                name = [x for x in item[1] if x.isalpha()]
                name_clean = ''.join(name)

                with open(f"json_files_staging/{item[0]}_{name_clean}_{item[3]}.json", "w") as write_file:
                    write_file.write(response_str)

            except requests.exceptions.JSONDecodeError:
                continue

    count += 1

dir_files = [file for file in os.listdir("json_files_staging")]
staging_files = [file.split('_')[0] for file in dir_files if file.split('.')[1] == 'json']

if len(staging_files) == 112:
    folder_path = "json_files_curr"
    shutil.rmtree(folder_path)
    os.rename("json_files_staging", "json_files_curr")
    os.mkdir("json_files_staging")
