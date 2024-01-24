import os
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Float, DateTime, String
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


class HourlyForecast(Base):

    __tablename__ = "hourlyforecasts"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    station = Column(String)
    time = Column(DateTime)
    temp_f = Column(Float)
    wind_mph = Column(Float)
    pressure_in = Column(Float)
    precip_in = Column(Float)
    humidity = Column(Float)
    cloud = Column(Float)
    feelslike_f = Column(Float)
    vis_miles = Column(Float)
    gust_mph = Column(Float)
    timestamp = Column(DateTime, default=datetime.now())


class Locations(Base):

    __tablename__ = "locations"
    __table_args__ = {'extend_existing': True}

    location_id = Column(Integer, primary_key=True)
    station = Column(String)
    usaf = Column(String)
    wban = Column(String)
    station_name = Column(String)
    ctry = Column(String)
    state = Column(String)
    icao = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    elevation = Column(Float)
    begin = Column(DateTime)
    end = Column(DateTime)
    timestamp = Column(DateTime, default=datetime.now())

DB_DIALECT = os.getenv('PG_AWK_DIALECT')
DB_USER = os.getenv('PG_AWK_USERNAME')
DB_PASSWORD = os.getenv('PG_AWK_PASSWORD')
DB_HOST = os.getenv('PG_AWK_HOST')
DB_PORT = os.getenv('PG_AWK_PORT')
DB_INSTANCE = os.getenv('PG_AWK_DB')

url_string = f"{DB_DIALECT}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_INSTANCE}"

if __name__ == "__main__":
    engine = create_engine(url=url_string, pool_size=5, pool_recycle=3600)
    Base.metadata.create_all(engine)
