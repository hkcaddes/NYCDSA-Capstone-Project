import requests
import time
import json
from sqlalchemy import *
import pandas as pd
import numpy as np
import datetime
pd.set_option('display.max_columns', 500)

df = pd.DataFrame()

city_list = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose',
            'New York', 'Brooklyn', 'Bronx', 'Flushing', 'East Rutherford',
           'Washington, DC', 'Vienna',
            'Chicago', 'Rosemont', 'Evanston',
           'Los Angeles', 'Hollywood', 'West Hollywood', 'Pasadena',
           'Boston', 'Medford']

for i in range(1,11):
    responseEvents = requests.get(f'https://api.seatgeek.com/events?per_page=5000&page={i}&client_id=MTI5NzU0MTd8MTUzNTkwMTU5Mi45Nw')

    eventsDict = json.loads(responseEvents.content.decode("utf-8")) #dictionary

    eventsList = eventsDict['events'] #list of ten dictionaries

    local_date = [eventsList[i]['datetime_local'] for i in range(len(eventsList))]
    utc_date = [eventsList[i]['datetime_utc'] for i in range(len(eventsList))]
    Id = [eventsList[i]['id'] for i in range(len(eventsList))]
    score = [eventsList[i]['score'] for i in range(len(eventsList))]
    title = [eventsList[i]['short_title'] for i in range(len(eventsList))]
    avgPrice = [eventsList[i]['stats']['average_price'] for i in range(len(eventsList))]
    hiPrice = [eventsList[i]['stats']['highest_price'] for i in range(len(eventsList))]
    listCount = [eventsList[i]['stats']['listing_count'] for i in range(len(eventsList))]
    lowPrice = [eventsList[i]['stats']['lowest_price'] for i in range(len(eventsList))]
    lowPriceDeals = [eventsList[i]['stats']['lowest_price_good_deals'] for i in range(len(eventsList))]
    Type = [eventsList[i]['type'] for i in range(len(eventsList))]
    city = [eventsList[i]['venue']['city'] for i in range(len(eventsList))]
    state = [eventsList[i]['venue']['state'] for i in range(len(eventsList))]
    lat = [eventsList[i]['venue']['location']['lat'] for i in range(len(eventsList))]
    lon = [eventsList[i]['venue']['location']['lon'] for i in range(len(eventsList))]
    postal = [eventsList[i]['venue']['postal_code'] for i in range(len(eventsList))]
    url = [eventsList[i]['url'] for i in range(len(eventsList))]
    performer = [eventsList[i]['performers'][0]['name'] for i in range(len(eventsList))]

    dfNew = pd.DataFrame({"local_date": local_date, "utc_date": utc_date, "Id": Id, "score": score, "title":title, 
                   "avgPrice": avgPrice, "hiPrice": hiPrice, "listCount": listCount, "lowPrice": lowPrice, 
                   "lowPriceDeals": lowPriceDeals, "Type":Type, "city": city, "state":state, 
                   "lat": lat, "lon": lon, "postal":postal, "url": url, "performer": performer})
                                  
    df = pd.concat([df, dfNew])

dfCity = df.loc[df.city.apply(lambda x: x in city_list)] #filter by city list
dfConcert = dfCity.loc[dfCity.Type == "concert"] #filter by Type == concert

today = datetime.date.today()

dfConcert.to_csv(f'SeatGeek_{today.month}_{today.day}_{today.year}.csv')

time.sleep(30)

############################################## MOVE FROM S3 TO REDSHIFT ########################################################

engine = create_engine(
    'postgresql+psycopg2://awsuser:Capstone1@redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')

# get day
month = dt.datetime.today().month
day = dt.datetime.today().day
year = dt.datetime.today().year

# =============================================================================================================
# SEATGEEK.VENUES_DF

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.sg_venues_df (LIKE seatgeek.venues_df);")

# fill working table
engine.execute(text("""COPY working.sg_venues_df FROM 's3://nycdsa.ta-am/SeatGeek_venueDF_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(month,day,year)).execution_options(autocommit=True))
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
engine.execute(text("""COPY working.sg_prices_df FROM 's3://nycdsa.ta-am/SeatGeek_pricesDF_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(month,day,year)).execution_options(autocommit=True))
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
engine.execute(text("""COPY working.sg_events_df FROM 's3://nycdsa.ta-am/SeatGeek_eventsDF_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(month,day,year)).execution_options(autocommit=True))
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