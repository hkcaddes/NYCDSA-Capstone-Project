from io import StringIO
import boto3
from botocore.client import Config
import requests
import json
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import *
import time

pd.set_option('display.max_columns', 500)

df = pd.DataFrame()

city_list = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose',
            'New York', 'Brooklyn', 'Bronx', 'Flushing', 'East Rutherford',
           'Washington, DC', 'Vienna',
            'Chicago', 'Rosemont', 'Evanston',
           'Los Angeles', 'Hollywood', 'West Hollywood', 'Pasadena',
           'Boston', 'Medford']

for i in range(1,11):
    responseEvents = requests.get('https://api.seatgeek.com/events?per_page=5000&page='+str(i)+'&client_id=MTI5NzU0MTd8MTUzNTkwMTU5Mi45Nw')

    eventsDict = json.loads(responseEvents.content.decode("utf-8")) #dictionary

    eventsList = eventsDict['events'] #list of ten dictionaries

    local_date = [eventsList[i]['datetime_local'] for i in range(len(eventsList))]
    utc_date = [eventsList[i]['datetime_utc'] for i in range(len(eventsList))]
    eventID = [eventsList[i]['id'] for i in range(len(eventsList))]
    score = [eventsList[i]['score'] for i in range(len(eventsList))]
    title = [eventsList[i]['short_title'] for i in range(len(eventsList))]
    avgPrice = [eventsList[i]['stats']['average_price'] for i in range(len(eventsList))]
    hiPrice = [eventsList[i]['stats']['highest_price'] for i in range(len(eventsList))]
    listCount = [eventsList[i]['stats']['listing_count'] for i in range(len(eventsList))]
    lowPrice = [eventsList[i]['stats']['lowest_price'] for i in range(len(eventsList))]
    lowPriceDeals = [eventsList[i]['stats']['lowest_price_good_deals'] for i in range(len(eventsList))]
    Type = [eventsList[i]['type'] for i in range(len(eventsList))]
    lat = [eventsList[i]['venue']['location']['lat'] for i in range(len(eventsList))]
    lon = [eventsList[i]['venue']['location']['lon'] for i in range(len(eventsList))]
    postal = [eventsList[i]['venue']['postal_code'] for i in range(len(eventsList))]
    url = [eventsList[i]['url'] for i in range(len(eventsList))]
    performer = [eventsList[i]['performers'][0]['name'] for i in range(len(eventsList))]
    venueCity = [eventsList[i]['venue']['city'] for i in range(len(eventsList))]
    venueState = [eventsList[i]['venue']['state'] for i in range(len(eventsList))]
    venueName = [eventsList[i]['venue']['name'] for i in range(len(eventsList))]
    venueID = [eventsList[i]['venue']['id'] for i in range(len(eventsList))]

    dfNew = pd.DataFrame({"local_date": local_date, "utc_date": utc_date, "eventID": eventID, "score": score, "title":title, 
        "avgPrice": avgPrice, "hiPrice": hiPrice, "listCount": listCount, "lowPrice": lowPrice, 
        "lowPriceDeals": lowPriceDeals, "Type":Type, "lat": lat, "lon": lon, "postal":postal, 
        "url": url, "performer": performer, "venueCity": venueCity, "venueState": venueState,
        "venueName": venueName, "venueID": venueID})
    
    df = pd.concat([df, dfNew])

today = datetime.date.today()

df = df.loc[df.venueCity.apply(lambda x: x in city_list)] #filter by city list
df = df.loc[df.Type == "concert"] #filter by Type == concert

eventsDF = df[['eventID','local_date','performer','title','utc_date', 'venueID']].drop_duplicates(subset = 'eventID')

venueDF = df[['venueID','venueName','venueCity','venueState','postal','lat','lon']].drop_duplicates(subset = 'venueID')

pricesDF = df[['eventID','avgPrice','hiPrice','listCount',
               'lowPrice','lowPriceDeals','score']].groupby('eventID').mean()

pricesDF = pricesDF[np.sum(pricesDF.isnull(), axis = 1) != 5].reset_index()
pricesDF['accessDate'] = today
pricesDF['accessTime'] = pd.Timestamp.now()
eventsDF['accessTime'] = pd.Timestamp.now()
venueDF['accessTime'] = pd.Timestamp.now()


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

#################################### PUSH EVENTS TO S3 #############################################################
eventsDF_buffer = StringIO()
eventsDF.to_csv(eventsDF_buffer, index=False)

s3.Bucket(BUCKET_NAME).put_object(Key='SeatGeek_eventsDF_'+str(today.month)+'_'+str(today.day)+'_'+str(today.year)+'.csv', Body=eventsDF_buffer.getvalue())

print('SeatGeek Events Uploaded to S3 '+str(today))


#################################### PUSH VENUES TO S3 #############################################################
venueDF_buffer = StringIO()
venueDF.to_csv(venueDF_buffer, index=False)

s3.Bucket(BUCKET_NAME).put_object(Key='SeatGeek_venueDF_'+str(today.month)+'_'+str(today.day)+'_'+str(today.year)+'.csv', Body=venueDF_buffer.getvalue())

print('SeatGeek Venues Uploaded to S3 '+str(today))

#################################### PUSH PRICES TO S3 #############################################################
pricesDF_buffer = StringIO()
pricesDF.to_csv(pricesDF_buffer, index=False)

s3.Bucket(BUCKET_NAME).put_object(Key='SeatGeek_pricesDF_'+str(today.month)+'_'+str(today.day)+'_'+str(today.year)+'.csv', Body=pricesDF_buffer.getvalue())

print('SeatGeek Prices Uploaded to S3 '+str(today))

time.sleep(30)

#################################### NOW PUSH S3 CSV's INTO REDSHIFT ###############################################

engine = create_engine(
    'postgresql+psycopg2://awsuser:Capstone1@redshift-cluster-1.cah6qt0iybst.us-east-2.redshift.amazonaws.com:5439/dev')

################################### SEATGEEK VENUES INTO REDSHIFT TABLE#############################################	

engine.execute("CREATE TABLE IF NOT EXISTS working.sg_venues_df (LIKE seatgeek.venues_df);")

# fill working table
engine.execute(text("""COPY working.sg_venues_df FROM 's3://nycdsa.ta-am/SeatGeek_venueDF_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(today.month, today.day, today.year)).execution_options(autocommit=True))
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

print('SeatGeek Venues Updated in Redshift '+str(today))

################################### SEATGEEK PRICES INTO REDSHIFT TABLE#############################################	

engine.execute("CREATE TABLE IF NOT EXISTS working.sg_prices_df (LIKE seatgeek.prices_df);")

# fill working table
engine.execute(text("""COPY working.sg_prices_df FROM 's3://nycdsa.ta-am/SeatGeek_pricesDF_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(today.month, today.day, today.year)).execution_options(autocommit=True))
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

print('SeatGeek Prices Updated in Redshift '+str(today))

################################### SEATGEEK PRICES INTO REDSHIFT TABLE#############################################	

# make sure working table exists
engine.execute("CREATE TABLE IF NOT EXISTS working.sg_events_df (LIKE seatgeek.events_df);")

# fill working table
engine.execute(text("""COPY working.sg_events_df FROM 's3://nycdsa.ta-am/SeatGeek_eventsDF_{}_{}_{}.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::148285915521:role/myRedshiftRole'
    DELIMITER ',' REGION 'us-east-2'
    IGNOREHEADER 1
    CSV;""".format(today.month, today.day, today.year)).execution_options(autocommit=True))
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

print('SeatGeek Events Updated in Redshift '+str(today))
