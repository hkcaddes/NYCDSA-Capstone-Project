# using sqlalchemy to create and update tables in redshift cluster

# Tables to create in this script:

# events_scores
# venues
# tickets_deliv_method
# tickets_deliv_type
# tickets_listing_attr

# Establish cluster connection
import pandas as pd
from sqlalchemy import *

# create engine
engine = create_engine(
    'postgresql+psycopg2://awsuser:Capstone1@redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')

# STUBHUB SCHEMA
engine.execute("CREATE SCHEMA IF NOT EXISTS stubhub;")




# ======================================================================================================================
# STUBHUB.EVENTS_SCORES
# 1:
 # create table
engine.execute("""CREATE TABLE stubhub.events_scores(
    event_id INTEGER,
    score FLOAT,
    dt_accessed TIMESTAMP,
    date_accessed CHAR (10));""")

# make sure it worked
pd.read_sql("SELECT * FROM stubhub.events_scores", engine)

# fill table
engine.execute(text("""COPY stubhub.events_scores FROM 's3://nycdsa.ta-am/events_scores_2018_09_08.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))
# check if copy command worked
pd.read_sql("SELECT COUNT(event_id) FROM stubhub.events_scores", engine)


# 2:
    # create working table
engine.execute("CREATE TABLE working.events_scores (LIKE stubhub.events_scores);")

# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(9, 12):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.events_scores FROM 's3://nycdsa.ta-am/events_scores_2018_09_{}.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
    # fill only if listing_id and listing_deliv_method is unique
    engine.execute(text("""INSERT INTO stubhub.events_scores
        SELECT w.*
        FROM working.events_scores AS w
        WHERE
            NOT EXISTS (SELECT 1
                FROM stubhub.events_scores AS s
                WHERE s.event_id = w.event_id);""").execution_options(autocommit=True))
    # empty working table
    engine.execute("TRUNCATE working.events_scores;")
    print("done")
# 4:
    # see final count
pd.read_sql("SELECT COUNT(event_id) FROM stubhub.events_scores;", engine)

# =========================================================================================================
# STUBHUB.VENUES
# 1:
    # create table
engine.execute("""CREATE TABLE stubhub.venues_df(
    venue_id INTEGER,
    name VARCHAR,
    url VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    timezone CHAR (3),
    jdk_timezone VARCHAR,
    address1 VARCHAR,
    city VARCHAR (50),
    state VARCHAR,
    postalcode INTEGER,
    country CHAR (2),
    dt_accessed TIMESTAMP,
    address2 VARCHAR);""")

# check
pd.read_sql("SELECT * FROM stubhub.venues_df", engine)

# fill table
engine.execute(text("""COPY stubhub.venues_df FROM 's3://nycdsa.ta-am/venues_df_2018_09_08.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))

# check
pd.read_sql("SELECT COUNT(venue_id) FROM stubhub.venues_df", engine)

# 2:
    # create working table
engine.execute("CREATE TABLE working.venues_df (LIKE stubhub.venues_df);")
pd.read_sql("SELECT * FROM working.venues_df", engine)
# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(9, 12):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.venues_df FROM 's3://nycdsa.ta-am/venues_df_2018_09_{}.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
    # fill only if listing_id and listing_deliv_method is unique
    engine.execute(text("""INSERT INTO stubhub.venues_df
        SELECT w.*
        FROM working.venues_df AS w
        WHERE
            NOT EXISTS (SELECT 1
                FROM stubhub.venues_df AS s
                WHERE s.name = w.name);""").execution_options(autocommit=True))
    # empty working table
    engine.execute("TRUNCATE working.venues_df;")
    print("done")

# count rows to make sure it all copied
pd.read_sql("SELECT COUNT(venue_id) FROM stubhub.venues_df;", engine)





# =============================================================================================
# STUBHUB.TICKETS_DELIV_METHOD
engine.execute("""CREATE TABLE stubhub.tickets_deliv_method(
    listing_id INTEGER,
    listing_deliv_method FLOAT,
    dt_accessed TIMESTAMP,
    date_accessed CHAR (10));""")

# check
pd.read_sql("SELECT * FROM stubhub.tickets_deliv_method", engine)

# fill table
engine.execute(text("""COPY stubhub.tickets_deliv_method FROM 's3://nycdsa.ta-am/tickets_deliv_method_2018_09_08.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))

# check if copy worked
pd.read_sql("SELECT COUNT(listing_id) FROM stubhub.tickets_deliv_method;", engine)


# create working table
engine.execute("CREATE TABLE working.tickets_deliv_method (LIKE stubhub.tickets_deliv_method);")


# loop through new days
# only add in new listings (new listing_id + new listing_deliv_method)
for x in range(9, 12):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.tickets_deliv_method FROM 's3://nycdsa.ta-am/tickets_deliv_method_2018_09_{}.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
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
    print("done")
# see final count
pd.read_sql("SELECT COUNT(listing_id) FROM stubhub.tickets_deliv_method;", engine)




# ===============================================================================================
# STUBHUB.TICKETS_DELIV_TYPE
# 1:
    # create table
engine.execute("""CREATE TABLE stubhub.tickets_deliv_type(
    listing_id INTEGER,
    listings_deliv_type FLOAT,
    dt_accessed TIMESTAMP,
    date_accessed CHAR (10));""")

# check
pd.read_sql("SELECT * FROM stubhub.tickets_deliv_type", engine)

# fill table
engine.execute(text("""COPY stubhub.tickets_deliv_type FROM 's3://nycdsa.ta-am/tickets_deliv_type_2018_09_08.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))

# check if copy worked
pd.read_sql("SELECT COUNT(listing_id) FROM stubhub.tickets_deliv_type;", engine)
# 2:
    # create working table
engine.execute("CREATE TABLE working.tickets_deliv_type (LIKE stubhub.tickets_deliv_type);")

# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(9, 12):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.tickets_deliv_type FROM 's3://nycdsa.ta-am/tickets_deliv_type_2018_09_{}.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
    # fill only if listing_id and listing_deliv_method is unique
    engine.execute(text("""INSERT INTO stubhub.tickets_deliv_type
        SELECT w.*
        FROM working.tickets_deliv_type AS w
        WHERE
            NOT EXISTS (SELECT 1
                FROM stubhub.tickets_deliv_type AS s
                WHERE s.listing_id = w.listing_id AND s.listings_deliv_type = w.listings_deliv_type);""").execution_options(autocommit=True))
    # empty working table
    engine.execute("TRUNCATE working.tickets_deliv_type;")
    print("done")

# 4:
    # see final count
pd.read_sql("SELECT COUNT(listing_id) FROM stubhub.tickets_deliv_type;", engine)




# =============================================================================================
# STUBHUB.TICKETS_LISTING_ATTR
# 1:
    # create table
engine.execute("""CREATE TABLE stubhub.tickets_listing_attr(
    listing_id INTEGER,
    listings_listing_attr FLOAT);""")

# check
pd.read_sql("SELECT * FROM stubhub.tickets_listing_attr", engine)

# fill table
engine.execute(text("""COPY stubhub.tickets_listing_attr FROM 's3://nycdsa.ta-am/tickets_listing_attr_2018_09_08.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))

# check if copy worked
pd.read_sql("SELECT COUNT(listing_id) FROM stubhub.tickets_listing_attr;", engine)


# 2:
    # create working table
engine.execute("CREATE TABLE working.tickets_listing_attr (LIKE stubhub.tickets_listing_attr);")

# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(9, 12):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.tickets_listing_attr FROM 's3://nycdsa.ta-am/tickets_listing_attr_2018_09_{}.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
    # fill only if listing_id and listing_deliv_method is unique
    engine.execute(text("""INSERT INTO stubhub.tickets_listing_attr
        SELECT w.*
        FROM working.tickets_listing_attr AS w
        WHERE
            NOT EXISTS (SELECT 1
                FROM stubhub.tickets_listing_attr AS s
                WHERE s.listing_id = w.listing_id AND s.listings_listing_attr = w.listings_listing_attr);""").execution_options(autocommit=True))
    # empty working table
    engine.execute("TRUNCATE working.tickets_listing_attr;")
    print("done")

# 4:
    # see final count
pd.read_sql("SELECT COUNT(listing_id) FROM stubhub.tickets_listing_attr;", engine)




# ==================================================================================================
# SEATGEEK
# ===================================================================================================

# SEATGEEK SCHEMA
engine.execute("CREATE SCHEMA IF NOT EXISTS seatgeek;")



# ======================================================================================================================
# SEATGEEK.VENUES_DF
# 1:
 # create table
engine.execute("""CREATE TABLE seatgeek.venues_df(
    venue_id INTEGER,
    name VARCHAR,
    city VARCHAR,
    state CHAR (2),
    postalcode INTEGER,
    latitude FLOAT,
    longitude FLOAT,
    dt_accessed TIMESTAMP);""")

# make sure it worked
pd.read_sql("SELECT * FROM seatgeek.venues_df", engine)

# fill table
engine.execute(text("""COPY seatgeek.venues_df FROM 's3://nycdsa.ta-am/SeatGeek_venuesDF_9_11_2018.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))
# check if copy command worked
pd.read_sql("SELECT COUNT(venue_id) FROM seatgeek.venues_df", engine)


# 2:
    # create working table
engine.execute("CREATE TABLE working.sg_venues_df (LIKE seatgeek.venues_df);")

# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(14, 15):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.sg_venues_df FROM 's3://nycdsa.ta-am/SeatGeek_venueDF_9_{}_2018.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
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
    print("done")


# 4:
    # see final count
pd.read_sql("SELECT COUNT(venue_id) FROM seatgeek.venues_df;", engine)
# still need 13 and 14

# ======================================================================================================================
# SEATGEEK.PRICES_DF
# 1:
 # create table
engine.execute("""CREATE TABLE seatgeek.prices_df(
    event_id INTEGER,
    avg_price FLOAT,
    hi_price FLOAT,
    list_count FLOAT,
    low_price FLOAT,
    low_price_deals FLOAT,
    score FLOAT,
    date_accessed CHAR (10),
    dt_accessed TIMESTAMP);""")

# make sure it worked
pd.read_sql("SELECT * FROM seatgeek.prices_df", engine)

# fill table
engine.execute(text("""COPY seatgeek.prices_df FROM 's3://nycdsa.ta-am/SeatGeek_pricesDF_9_11_2018.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))
# check if copy command worked
pd.read_sql("SELECT COUNT(event_id) FROM seatgeek.prices_df", engine)

# 2:
    # create working table
engine.execute("CREATE TABLE working.sg_prices_df (LIKE seatgeek.prices_df);")

# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(12, 14):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.sg_prices_df FROM 's3://nycdsa.ta-am/SeatGeek_pricesDF_9_{}_2018.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
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
    print("done")


# 4:
    # see final count
pd.read_sql("SELECT COUNT(event_id) FROM seatgeek.prices_df;", engine)


# ======================================================================================================================
# SEATGEEK.EVENTS_DF
# 1:
 # create table
engine.execute("""CREATE TABLE seatgeek.events_df(
    event_id INTEGER,
    local_date CHAR (19),
    performer VARCHAR,
    title VARCHAR,
    utc_date CHAR (19),
    venue_id INTEGER,
    dt_acessed TIMESTAMP);""")

# make sure it worked
pd.read_sql("SELECT * FROM seatgeek.events_df", engine)

# fill table
engine.execute(text("""COPY seatgeek.events_df FROM 's3://nycdsa.ta-am/SeatGeek_eventsDF_9_11_2018.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""").execution_options(autocommit=True))
# check if copy command worked
pd.read_sql("SELECT COUNT(event_id) FROM seatgeek.events_df", engine)

# 2:
    # create working table
engine.execute("CREATE TABLE working.sg_events_df (LIKE seatgeek.events_df);")

# 3:
    # loop through new days
    # only add in new listings (new listing_id + new listings_deliv_type)
for x in range(13, 14):
    if x < 10:
        x = '0{}'.format(x)
    else:
        x = str(x)
    print(x)
    # fill working table
    engine.execute(text("""COPY working.sg_events_df FROM 's3://nycdsa.ta-am/SeatGeek_eventsDF_9_{}_2018.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
        DELIMITER ',' REGION 'us-east-2'
        IGNOREHEADER 1
        CSV;""".format(x)).execution_options(autocommit=True))
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
    print("done")


# 4:
    # see final count
pd.read_sql("SELECT COUNT(event_id) FROM seatgeek.events_df;", engine)
