from io import StringIO
import boto3
from botocore.client import Config
import stubhub_scraper
import pandas as pd
import importlib
from sqlalchemy import *
import datetime as dt
import time

_ = importlib.reload(stubhub_scraper)

scraper = stubhub_scraper.StubHub_Scrape(test_mode=False)
scraper.get_tickets()

s3_credentials = open('s3_credentials.txt', 'r')
ACCESS_SECRET = s3_credentials.readlines()
ACCESS_KEY = ACCESS_SECRET[1][20:-1]
SECRET_KEY = ACCESS_SECRET[2][24:]
s3_credentials.close()

BUCKET_NAME = 'nycdsa.ta-am'

s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4')
)
    
# Save dfs as CSVs
combined_df_dict = {**scraper.events, **scraper.tickets}
for key in combined_df_dict:
    df = combined_df_dict[key]
    key_splits = key.split('_')
    schema = key_splits[0]
    table_name = '_'.join(key_splits[1:])
    file_name = '{}_{}_{}.csv'.format(schema,table_name, scraper.scrape_date)
    BUFFER = StringIO()
    try:
        df.to_csv(BUFFER, index=False)
    except:
        pass
    s3.Bucket(BUCKET_NAME).put_object(Key=file_name, Body=BUFFER.getvalue())    
    print('{} written'.format(file_name))

print('Stubhub Data Uploaded to S3')

time.sleep(30)

############################# UPDATE STUBHUB DATA IN REDSHIFT ##############################################

engine = create_engine(
    'postgresql+psycopg2://awsuser:Capstone1@redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')

# get day
if dt.datetime.today().day < 10:
    day = "0{}".format(dt.datetime.today().day)
else:
    day = dt.datetime.today().day
# year
year = dt.datetime.today().year

# month
if dt.datetime.today().month < 10:
    month = "0{}".format(dt.datetime.today().month)
else:
    month = dt.datetime.today().month

# =============================================================================================================

# EVENTS

# =============================================================================================================
# STUBHUB.EVENTS_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.events_df (LIKE stubhub.events_df);")

# fill working table
engine.execute(text("""COPY working.events_df FROM 's3://nycdsa.ta-am/events_df_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if event_id is unique
engine.execute(text("""INSERT INTO stubhub.events_df
    SELECT w.*
    FROM working.events_df AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.events_df AS s
            WHERE s.event_id = w.event_id);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.events_df;")


# =============================================================================================================
# STUBHUB.EVENTS_PERF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.events_perf (LIKE stubhub.events_perf);")

# fill working table
engine.execute(text("""COPY working.events_perf FROM 's3://nycdsa.ta-am/events_perf_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if event_id and performer_id are unique
engine.execute(text("""INSERT INTO stubhub.events_perf
    SELECT w.*
    FROM working.events_perf AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.events_perf AS s
            WHERE s.event_id = w.event_id AND s.performer_id = w.performer_id);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.events_perf;")


# =============================================================================================================
# STUBHUB.EVENTS_TICKET_SUMMARY

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.events_ticket_summary (LIKE stubhub.events_ticket_summary);")

# fill working table
engine.execute(text("""COPY working.events_ticket_summary FROM 's3://nycdsa.ta-am/events_ticket_summary_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill into original table
engine.execute(text("""ALTER TABLE stubhub.events_ticket_summary APPEND FROM working.events_ticket_summary;""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.events_ticket_summary;")

# =============================================================================================================
# STUBHUB.EVENTS_SCORES

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.events_scores (LIKE stubhub.events_scores);")

# fill working table
engine.execute(text("""COPY working.events_scores FROM 's3://nycdsa.ta-am/events_scores_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if event_id is unique
engine.execute(text("""INSERT INTO stubhub.events_scores
    SELECT w.*
    FROM working.events_scores AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.events_scores AS s
            WHERE s.event_id = w.event_id);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.events_scores;")
# =============================================================================================================

# TICKETS

# =============================================================================================================
# STUBHUB.TICKETS_DELIV_METHOD

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.tickets_deliv_method (LIKE stubhub.tickets_deliv_method);")

# fill working table
engine.execute(text("""COPY working.tickets_deliv_method FROM 's3://nycdsa.ta-am/tickets_deliv_method_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if listing_id and listing_deliv_method is unique
engine.execute(text("""INSERT INTO stubhub.tickets_deliv_method
    SELECT w.*
    FROM working.tickets_deliv_method AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.tickets_deliv_method AS s
            WHERE s.listing_id = w.listing_id AND s.listing_deliv_method = w.listing_deliv_method);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.tickets_deliv_method;")

# =============================================================================================================
# STUBHUB.TICKETS_DELIV_TYPE

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.tickets_deliv_type (LIKE stubhub.tickets_deliv_type);")

# fill working table
engine.execute(text("""COPY working.tickets_deliv_type FROM 's3://nycdsa.ta-am/tickets_deliv_type_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if listing_id and listing_deliv_type is unique
engine.execute(text("""INSERT INTO stubhub.tickets_deliv_type
    SELECT w.*
    FROM working.tickets_deliv_type AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.tickets_deliv_type AS s
            WHERE s.listing_id = w.listing_id AND s.listings_deliv_type = w.listings_deliv_type);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.tickets_deliv_type;")

# =============================================================================================================
# STUBHUB.TICKETS_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.tickets_df (LIKE stubhub.tickets_df);")

# fill working table
engine.execute(text("""COPY working.tickets_deliv_type FROM 's3://nycdsa.ta-am/tickets_deliv_type_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill into original table
engine.execute(text("""ALTER TABLE stubhub.tickets_df APPEND FROM working.tickets_df;""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.tickets_df;")
# =============================================================================================================
# STUBHUB.TICKETS_LISTING_ATTR

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.tickets_listing_attr (LIKE stubhub.tickets_listing_attr);")

# fill working table
engine.execute(text("""COPY working.tickets_listing_attr FROM 's3://nycdsa.ta-am/tickets_listing_attr_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if listing_id and listing_listings_attr is unique
engine.execute(text("""INSERT INTO stubhub.tickets_listing_attr
    SELECT w.*
    FROM working.tickets_listing_attr AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.tickets_listing_attr AS s
            WHERE s.listing_id = w.listing_id AND s.listings_listing_attr = w.listings_listing_attr);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.tickets_listing_attr;")

# =============================================================================================================
# STUBHUB.TICKETS_SPLITS

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.tickets_splits (LIKE stubhub.tickets_splits);")

# fill working table
engine.execute(text("""COPY working.tickets_splits FROM 's3://nycdsa.ta-am/tickets_splits_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if listing_id and listing_listings_attr is unique
engine.execute(text("""ALTER TABLE stubhub.tickets_splits APPEND FROM working.temp_tickets_splits;""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.tickets_splits;")
# =============================================================================================================

# VENUES

# =============================================================================================================
# STUBHUB.VENUES_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.venues_df (LIKE stubhub.venues_df);")

# fill working table
engine.execute(text("""COPY working.venues_df FROM 's3://nycdsa.ta-am/venues_df_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(year, month, day)).execution_options(autocommit=True))

# fill only if name is unique
engine.execute(text("""INSERT INTO stubhub.venues_df
    SELECT w.*
    FROM working.venues_df AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM stubhub.venues_df AS s
            WHERE s.name = w.name);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.venues_df;")

print('StubHub Data Updated in Redshift')