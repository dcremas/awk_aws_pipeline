import os
import sys
import unittest
from datetime import datetime, timezone, timedelta
import io
from contextlib import redirect_stdout
import pytz
import boto3
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
import shared_funcs


def main(out=sys.stdout, verbosity = 2):
    loader = unittest.TestLoader()
  
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class TestS3AWK(unittest.TestCase):

    @classmethod
    def setUp(self):
        # Calculate the file counts for the s3 AWK buckets.
        self.s3_staging_len = len(s3_dir_list_staging)
        self.s3_current_len = len(s3_dir_list_current)
        # Calculate the date creation times for the s3 AWK buckets.
        self.s3_staging_lastupdate = len(list(
            filter(lambda x: x > 4, map(lambda x: int((tz_aware_now - x[1]).seconds / 3600), s3_dir_list_staging))))
        self.s3_current_lastupdate = len(list(
            filter(lambda x: x > 4, map(lambda x: int((tz_aware_now - x[1]).seconds / 3600), s3_dir_list_current))))
        # Calculate the file sizes for the s3 AWK buckets.
        self.s3_staging_filesizes = len(list(
            filter(lambda x: x < 300000, map(lambda x: int(x[2]), s3_dir_list_staging))))
        self.s3_current_filesizes = len(list(
            filter(lambda x: x < 300000, map(lambda x: int(x[2]), s3_dir_list_current))))
        with engine.connect() as connection:
            statement_count = text(count_query)
            result_count = connection.execute(statement_count)
            self.db_record_count = [x for x in result_count][0][0]
            statement_date = text(date_query)
            result_date = connection.execute(statement_date)
            result_date_clean = [x for x in result_date][0][0] - timedelta(hours = 6)
            self.db_hour_diff = int((datetime.now() - result_date_clean).seconds / 3600)
        
    def test_staging_file_count(self):
        self.assertEqual(self.s3_staging_len, 112, msg=f"""
        The AWK Staging Bucket Does Not Have Enough Files,
        Only {self.s3_staging_len} Are Availalbe.
        """)

    def test_current_file_count(self):
        self.assertEqual(self.s3_current_len, 112, msg=f"""
        The AWK Current Bucket Does Not Have Enough Files.
        Only {self.s3_current_len} Are Availalbe.
        """)

    def test_staging_files_lastupdate(self):
        self.assertEqual(self.s3_staging_lastupdate, 0, msg=f"""
        The AWK Staging Files Have Not Updated Properly.
        {self.s3_staging_lastupdate} Files Haven't Updated.
        """)

    def test_current_files_lastupdate(self):
        self.assertEqual(self.s3_current_lastupdate, 0, msg=f"""
        The AWK Current Files Have Not Updated Properly.
        {self.s3_current_lastupdate} Files Haven't Updated.
        """)

    def test_staging_filesizes(self):
        self.assertEqual(self.s3_staging_filesizes, 0, msg=f"""
        The AWK Staging Files Are Not All Big Enough.
        {self.s3_staging_filesizes} Files Are An Issue.
        """)

    def test_current_filesizes(self):
        self.assertEqual(self.s3_current_filesizes, 0, msg=f"""
        The AWK Current Files Are Not All Big Enough.
        {self.s3_current_filesizes} Files Are An Issue.
        """)

    def test_db_recordcount(self):
        self.assertEqual(self.db_record_count, 53760, msg=f"""
        The Postgresql Database Is Not The Right SIze.
        Only {self.db_record_count} Records Are Currently In The Database.
        """)

    def test_db_lastupdate(self):
        self.assertLess(self.db_hour_diff, 5, msg=f"""
        The Postgresql Datbase Is Not Updating Correctly.
        The Last Update Was {self.db_hour_diff} Hours Ago.
        """)

load_dotenv()

S3_BUCKET_NAME = 'apple-weatherkit'
S3_STAGING_PREFIX = 'json_files_staging/'
S3_CURRENT_PREFIX = 'json_files_current/'

s3_client = boto3.client('s3')

s3_dir_list_staging = shared_funcs.s3_bucket_list(
    S3_BUCKET_NAME,
    S3_STAGING_PREFIX,
    s3_client
    )

s3_dir_list_current = shared_funcs.s3_bucket_list(
    S3_BUCKET_NAME,
    S3_STAGING_PREFIX,
    s3_client
    )

DB_DIALECT = os.getenv('PG_AWK_DIALECT')
DB_USER = os.getenv('PG_AWK_USERNAME')
DB_PASSWORD = os.getenv('PG_AWK_PASSWORD')
DB_HOST = os.getenv('PG_AWK_HOST')
DB_PORT = os.getenv('PG_AWK_PORT')
DB_INSTANCE = os.getenv('PG_AWK_DB')

url_string = f"{DB_DIALECT}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_INSTANCE}"

engine = create_engine(url=url_string, pool_size=5, pool_recycle=3600)

count_query = f"SELECT COUNT(*) FROM hourlyforecasts;"
date_query = f"SELECT MIN(timestamp) FROM hourlyforecasts LIMIT 1;"

date_string = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
tz_aware_now = datetime.now(pytz.timezone('America/Chicago'))

if __name__ == "__main__":
    with open(f"logs/{date_string}_testing.log", "w") as write_file:
        f = io.StringIO()
        with redirect_stdout(f):
            main(f)
        out = f.getvalue()
        write_file.write(out)
