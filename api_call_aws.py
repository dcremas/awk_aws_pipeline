import os
import sys
import csv
import json
from datetime import date, timedelta
import boto3
import requests
from dotenv import load_dotenv
from weatherkit.client import WKClient

sys.path.append('/opt/python')

import shared_funcs


def lambda_handler(event, context):

    load_dotenv('/opt/python/.env')

    S3_BUCKET_NAME = 'apple-weatherkit'
    S3_STAGING_PREFIX = 'json_files_staging/'
    S3_METADATA_LOC = 'metadata/location_extract.csv'

    s3_client = boto3.client('s3')

    s3_dir_list = shared_funcs.s3_bucket_list(
        S3_BUCKET_NAME,
        S3_STAGING_PREFIX,
        s3_client
        )
        
    delete_list = [{'Key': f"{S3_STAGING_PREFIX}{file}"} for file in s3_dir_list]
    delete_keys = {'Objects': delete_list}
    
    delete_response = s3_client.delete_objects(
        Bucket=S3_BUCKET_NAME, 
        Delete=delete_keys
        )
        
    s3_dir_list = shared_funcs.s3_bucket_list(
        S3_BUCKET_NAME,
        S3_STAGING_PREFIX,
        s3_client
        )

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
    params = {
        "timezone": TIMEZONE,
        "dataSets": ",".join(DATASETS),
        "currentAsOf": f'{year_tod}-{month_tod}-{day_tod}T00:00:00Z',
        "hourlyStart": f'{year_beg}-{month_beg}-{day_beg}T00:00:00Z',
        "hourlyEnd": f'{year_end}-{month_end}-{day_end}T00:00:00Z',
        "countryCode": 'US'
        }

    metadata_obj = s3_client.get_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_METADATA_LOC
        )
    metadata = metadata_obj['Body'].read().decode('utf-8').splitlines()
    metadata_records = csv.reader(metadata, delimiter='|')
    reader_data = [record for idx, record in enumerate(metadata_records) if idx > 0]

    count = 0
    
    while (len(s3_dir_list) < 112) and (count < 10):

        staging_files = [file.split('_')[0] for file in s3_dir_list if file.split('.')[1] == 'json']

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
                    if len(response_str) < 100000:
                        continue
                    else:
                        
                        name = [x for x in item[1] if x.isalpha()]
                        name_clean = ''.join(name)

                        s3_key = f"{S3_STAGING_PREFIX}{item[0]}_{name_clean}_{item[3]}.json"
                        s3_response = s3_client.put_object(
                            Body=response_str,
                            Bucket=S3_BUCKET_NAME,
                            Key=s3_key
                            )

                except requests.exceptions.JSONDecodeError:
                    continue
                
        s3_dir_list = shared_funcs.s3_bucket_list(
            S3_BUCKET_NAME,
            S3_STAGING_PREFIX, s3_client
            )
        
        count += 1

    print(f"The total number of attempts was : {count}")

    return
