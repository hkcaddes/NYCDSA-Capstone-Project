# script to update seetgeek tables daily


# Establish cluster connection
import pandas as pd
from sqlalchemy import *
import datetime as dt

# create engine
engine = create_engine(
    'postgresql+psycopg2://awsuser:Capstone1@redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')

# get day
day = dt.datetime.today().day

# =============================================================================================================
# SEATGEEK.VENUES_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.sg_venues_df (LIKE seatgeek.venues_df);")

# fill working table
engine.execute(text("""COPY working.sg_venues_df FROM 's3://nycdsa.ta-am/SeatGeek_venueDF_9_{}_2018.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(day)).execution_options(autocommit=True))
# fill only if listing_id and listing_deliv_method is unique
engine.execute(text("""INSERT INTO seatgeek.venues_df
    SELECT w.*
    FROM working.sg_venues_df AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM seatgeek.venues_df AS s
            WHERE s.venue_id = w.venue_id);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.sg_venues_df;")



# =============================================================================================================
# SEATGEEK.PRICES_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.sg_prices_df (LIKE seatgeek.prices_df);")

# fill working table
engine.execute(text("""COPY working.sg_prices_df FROM 's3://nycdsa.ta-am/SeatGeek_pricesDF_9_{}_2018.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(day)).execution_options(autocommit=True))
# fill only if listing_id and listing_deliv_method is unique
engine.execute(text("""INSERT INTO seatgeek.prices_df
    SELECT w.*
    FROM working.sg_prices_df AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM seatgeek.prices_df AS s
            WHERE s.event_id = w.event_id);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.sg_prices_df;")


# =============================================================================================================
# SEATGEEK.EVENTS_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.sg_events_df (LIKE seatgeek.events_df);")

# fill working table
engine.execute(text("""COPY working.sg_events_df FROM 's3://nycdsa.ta-am/SeatGeek_eventsDF_9_{}_2018.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(day)).execution_options(autocommit=True))
# fill only if listing_id and listing_deliv_method is unique
engine.execute(text("""INSERT INTO seatgeek.events_df
    SELECT w.*
    FROM working.sg_events_df AS w
    WHERE
        NOT EXISTS (SELECT 1
            FROM seatgeek.events_df AS s
            WHERE s.event_id = w.event_id);""").execution_options(autocommit=True))
# empty working table
engine.execute("TRUNCATE working.sg_events_df;")
