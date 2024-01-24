import os
import json
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from dotenv import load_dotenv
from db_create_main import HourlyForecast
import shared_funcs

load_dotenv()

db_log = shared_funcs.logger('logs/db_dml.log')

json_files = [x for x in os.listdir('json_files_curr') if x != '.DS_Store']
weather_hourly = list()

for file in json_files:

    station = file.split('_')[0]

    with open(f"json_files_curr/{file}", 'r') as data:
        weather_data = json.load(data)

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

db_insert_length = len(weather_hourly)

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
    db_log.info(f"The database has been updated with {db_insert_length} records.")
except Exception as err:
    db_log.error(f"The database was not updated, the exception type is: {type(err)}")
