import os
import sys
import json
import boto3
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from dotenv import load_dotenv

sys.path.append('/opt/python')

from db_ddl import HourlyForecast
import shared_funcs


def lambda_handler(event, context):

    load_dotenv('/opt/python')

    S3_BUCKET_NAME = 'apple-weatherkit'
    S3_STAGING_PREFIX = 'json_files_staging/'
    S3_CURR_PREFIX = 'json_files_curr/'

    s3_dir_list = shared_funcs.s3_bucket_list(
        S3_BUCKET_NAME,
        S3_STAGING_PREFIX,
        s3_client
        )

    for file in s3_dir_list:
        copy_source = f"{S3_BUCKET_NAME}/{S3_STAGING_PREFIX}{file}"
        key = f"{S3_CURR_PREFIX}{file}"
        response = s3_client.copy_object(
            Bucket=S3_BUCKET_NAME,
            CopySource=copy_source,
            Key=key,
            )

    session = boto3.Session(
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        )
    s3_session = session.resource('s3')

    json_files = shared_funcs.s3_bucket_list(
        S3_BUCKET_NAME,
        S3_CURR_PREFIX,
        s3_client
        )

    weather_hourly = list()

    for file in json_files:

        station = file.split('_')[0]

        response = s3_session.Object(S3_BUCKET_NAME, f"{S3_CURR_PREFIX}{file}")
        file_content = response.get()['Body'].read().decode('utf-8') 
        weather_data = json.loads(file_content)

        station = file.split('_')[0]
        
        for period in weather_data['forecastHourly']['hours']:
            temp_dict = dict()
            temp_dict['station'] = station
            temp_dict["time"] = period["forecastStart"]
            temp_dict["temp_f"] = shared_funcs.celsius_to_fahrenheit(period["temperature"])
            temp_dict["wind_mph"] = period["windSpeed"]
            temp_dict["pressure_in"] = shared_funcs.millibar_to_hg(period["pressure"])
            temp_dict["precip_in"] = shared_funcs.millimeters_to_inches(period["precipitationAmount"])
            temp_dict["humidity"] = period["humidity"]
            temp_dict["cloud"] = period["cloudCover"]
            temp_dict["feelslike_f"] = shared_funcs.celsius_to_fahrenheit(period["temperatureApparent"])
            temp_dict["vis_miles"] = shared_funcs.meters_to_miles(period["visibility"])
            temp_dict["gust_mph"] = period["windGust"]
            weather_hourly.append(temp_dict)

    DB_DIALECT = os.getenv('PG_AWK_DIALECT')
    DB_USER = os.getenv('PG_AWK_USERNAME')
    DB_PASSWORD = os.getenv('PG_AWK_PASSWORD')
    DB_HOST = os.getenv('PG_AWK_HOST')
    DB_PORT = os.getenv('PG_AWK_PORT')
    DB_INSTANCE = os.getenv('PG_AWK_DB')

    url_string = f"{DB_DIALECT}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_INSTANCE}"

    engine = create_engine(url=url_string, pool_size=5, pool_recycle=3600)

    delete_query = f"DELETE FROM hourlyforecasts;"

    with engine.connect() as connection:
        statement = text(delete_query)
        result = connection.execute(statement)
        connection.commit()

    try:
        session = Session(bind=engine)
        session.execute(insert(HourlyForecast), weather_hourly)
        session.commit()
    except Exception as err:
        pass

    return
