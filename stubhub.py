from io import StringIO
import boto3
from botocore.client import Config
import stubhub_scraper
import pandas as pd
#import pickle
import importlib
#import datetime
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

#today = datetime.date.today()


#forget this for now
######################################PUSH THIS TO S3 INSTEAD OF DUMPING LOCALLY##############################
# Pickle json / dict format in case I want to reprocess later
#with open('events_{}'+'_'+str(today.year)+'_'+str(today.month)+'_'+str(today.day)+'.json'.format(scraper.scrape_date), 'wb') as file:
#    pickle.dump(scraper._events_raw, file)
#with open('inventory_{}.json'.format(scraper.scrape_date), 'wb') as file:
#    pickle.dump(scraper._inventory_raw, file)
##############################################################################################################
    
    
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