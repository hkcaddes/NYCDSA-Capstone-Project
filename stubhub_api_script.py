# import required packages
import requests
import json
import base64
import pandas as pd
import numpy as np
import re
from time import sleep
import datetime


class stubhub_scrape(object):

    # Initialize class
    def __init__(self, sleep_time=6, test_mode = False):

        # Set the amount of time to sleep after each call
        self.__sleep_time = sleep_time

        # Store test mode indicator
        self.__test_mode = test_mode

        # Store date of initialization
        self.scrape_date = datetime.datetime.now().strftime('%Y_%m_%d')

        # Store the access token... initialize as none
        self.__headers = None
        self._gen_auth_header_()

        # Save the events dataframe... initialize as none
        self._events_raw = None
        self.events = None

        # Save the listings dataframe... initialize as none
        self._inventory_raw = None
        self.tickets = None

        # Set the list of cities we are searching for
        city_list = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose',
             'New York', 'Brooklyn', 'Bronx', 'Flushing', 'East Rutherford',
            'Washington, DC', 'Vienna',
             'Chicago', 'Rosemont', 'Evanston',
            'Los Angeles', 'Hollywood', 'West Hollywood', 'Pasadena',
            'Boston', 'Medford']
        self.__city_list = '"' + '"  |"'.join(city_list) + '"'




    # Method to generate headers with authentication
    def _gen_auth_header_(self):

        # Get credentials from txt file
        with open('./passwords.txt') as passwords:
            text = passwords.readlines()

            app_token = re.search("'.*", text[0]).group().replace("'", "")
            consumer_key = re.search("'.*", text[2]).group().replace("'", "")
            consumer_secret = re.search("'.*", text[3]).group().replace("'", "")
            stubhub_username = re.search("'.*", text[5]).group().replace("'", "")
            stubhub_password = re.search("'.*", text[6]).group().replace("'", "")

        # Generate and encode token
        combo = consumer_key + ':' + consumer_secret
        basic_authorization_token = base64.b64encode(combo.encode('utf-8'))

        # Build request header
        headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Authorization':'Basic '+basic_authorization_token.decode('utf-8')}

        # Build request body for authentication
        body = {
                'grant_type':'password',
                'username':stubhub_username,
                'password':stubhub_password,
                'scope': 'PRODUCTION'}

        # Make request for authentication
        url = 'https://api.stubhub.com/login'
        r = requests.post(url, headers=headers, data=body)
        print('authentication: {}'.format(r.status_code))

        # If authentication worked, parse the response object
        if r.status_code == 200:
            # Extract access token and user key
            token_respoonse = r.json()
            access_token = token_respoonse['access_token']
            user_GUID = r.headers['X-StubHub-User-GUID']

        # Otherwise, raise error
        else:
            print('authentication failed!')
            raise

        # Add auth token to headers
        headers['Authorization'] = 'Bearer ' + access_token
        headers['Accept'] = 'application/json'
        headers['Accept-Encoding'] = 'application/json'

        # Save header with auth
        self.__headers = headers

        # Take a rest!
        sleep(self.__sleep_time)


    # Method to get raw events list in dict form
    def _get_events_raw(self):

        # Define events url and query params
        events_url = 'https://api.stubhub.com/search/catalog/events/v3'
        params = {'city': self.__city_list, 'q': 'concert',
         'start': 0, 'rows': 500, 'fieldList': '*,ticketInfo'}

        # Run the first request; get the total number of events
        events_r = requests.get(events_url, headers = self.__headers, params = params).json()
        n_found = events_r['numFound']
        print('Got first page of events. Found {} total events.'.format(n_found))

        # Start collecting the event response objects; mark with the datetime collected
        events =  events_r['events']
        list(map(lambda i: i.update({'dt_accessed': str(datetime.datetime.now())}), events))


        # Take a rest! Then, get ready for the next request
        sleep(self.__sleep_time)
        params['start'] += 500

        # Run a loop through search results and collect
        while params['start'] < n_found:

            # Get results, mark with dt, and store
            events_r = requests.get(events_url, headers = self.__headers, params = params).json()['events']
            list(map(lambda i: i.update({'dt_accessed': str(datetime.datetime.now())}), events_r))
            events.extend(events_r)
            print('Got events {} through {}'.format(params['start'], params['start'] + 500))

            # Increment through pages; sleep before moving on
            params['start'] += 500
            sleep(self.__sleep_time)

            # Escape the while loop after two calls if in test mode
            if self.__test_mode and params['start']>1000:
                print('Stopping gathering events... test mode!')
                break

        print('Got events!')

        # Save raw events object
        self._events_raw = events



    def _parse_events(self):

        print('Parsing events.')

        # Turn events list into a dataframe and remove duplicate events
        events_df = pd.DataFrame(self._events_raw)
        events_df = events_df.drop_duplicates(subset='id')

        # Remove parking passes
        parking_passes = events_df.name.apply(lambda n:
                        re.search('parking passes only', n.lower()) != None)
        events_df = events_df[~parking_passes]

        # Extract event category
        events_df['category'] = events_df.categoriesCollection.apply(lambda x: x[0]['name'])

        # Boolean for event parking
        events_df['event_parking'] = events_df.eventAttributes.notna()

        # Event geo -- get the most detailed category
        events_df['geos'] = events_df.geos.apply(lambda x: x[-1]['name'])

        # Generate event performers dataframe
        events_df['performersCollection'] = events_df.performersCollection.fillna('none')
        events_df.apply(lambda event: None if event['performersCollection'] == 'none'
                                    else
                                        [perf.update({'event_id': event['id']})
                                                for perf in event['performersCollection']], axis=1)
        events_perf = pd.DataFrame(
                            events_df.loc[events_df['performersCollection'] != 'none', 'performersCollection'].
                                        apply(pd.Series).stack().tolist())
        events_perf = events_perf.rename(index=str, columns={'id': 'performer_id', 'name': 'performer_name'})

        # Generate event ticket summary dataframe
        # Add the event id and dt accessed to the dictionary
        events_df['ticketInfo'] = events_df.ticketInfo.fillna('none')
        events_df.apply(lambda event: None if event['ticketInfo'] == 'none'
                                            else event['ticketInfo'].update(
                                                        {'event_id': event['id'],
                                                        'dt_accessed': event['dt_accessed']})
                                                , axis=1)
        events_ticket_summary = events_df[events_df.ticketInfo.notnull()].\
                                                        ticketInfo.apply(pd.Series)
        # Remove unnecessary dictionary-like fields from dataframe
        events_ticket_summary = events_ticket_summary.loc[:, events_ticket_summary.columns.str.find('With') == -1]

        # Generate venues DataFrame
        venues = events_df.venue.apply(pd.Series)
        venues = venues.drop_duplicates(subset = 'id')
        venues = venues.rename(index=str, columns={'id': 'venue_id'})

        # Label events dataframe with venue id
        events_df['venue_id'] = events_df.venue.apply(lambda i: i['id'])

        # Get name of venue configuration
        events_df['venue_config'] = events_df.venueConfiguration.apply(lambda x: x['name'])

        # Drop unwanted columns
        events_df = events_df.drop(['ancestors', 'associatedEvents', 'bobId',
                                    'catalogTemplate', 'categories', 'categoriesCollection',
                                    'displayAttributes','eventAttributes', 'groupings',
                                    'groupingsCollection', 'imageUrl', 'images', 'locale',
                                    'mobileAttributes', 'performers', 'performersCollection',
                                    'sourceId', 'status', 'ticketInfo','venue', 'venueConfiguration'],
                                     axis = 1)

        events_df = events_df.rename(index=str, columns={'id': 'event_id'})

        # Save events dataframes
        self.events = {'events_df': events_df, 'events_perf': events_perf,
                        'events_ticket_summary': events_ticket_summary}

        print('Events parsed!')

    # End-to-end wrapper to generate a list of events
    def get_events(self):

        if not self._events_raw:
            self._get_events_raw()

        self._parse_events()

        print('We have events!')




    # Method to gather ticket inventory (json objects)
    def _get_inventory(self):

        # Get events if we have not yet
        if not self.events:
            self.get_events()

        print('Getting inventory.')

        # Get a list of event IDs that have available tickets
        events_l = self.events['events_ticket_summary']
        events_l = events_l.loc[events_l['totalListings']>0, ['event_id', 'totalListings']]
        events_l = events_l.reset_index(drop=True)
        print(events_l.head())

        # Save inventory URL
        inventory_url = 'https://api.stubhub.com/search/inventory/v2'

        # Initialize empty inventory list
        inventory = []

        # Only keep first n listings if we're in test mode
        if self.__test_mode:
            n = 3 # First n listings for testing
            events_l = events_l.iloc[0:n, :]

        # Loop through each event and get ticket listings
        for i, id, n_tickets in events_l.itertuples():

            print('Getting ticket inventory event {} out of {}'.format(i + 1, len(events_l)))

            # Initialize parameters and blank inventory list
            params = {'eventid': id, 'start': 0, 'rows': 250,
                    'fieldsList': '*,faceValue,listingAttributeList'}

            # Continue looping until we've gotten all of the listings
            while params['start'] <= n_tickets:

                # Use a try statement because sometimes the last listing
                # might be sold before we make the inventory query
                try:

                    # Query for tickets
                    inventory_r = requests.get(inventory_url, headers=self.__headers,
                        params=params).json()['listing']

                    # Add the event id to each ticket
                    list(map(lambda i: i.update({'event_id': id,
                                        'dt_accessed': str(datetime.datetime.now())}),
                                         inventory_r))

                    # Add responses to the inventory list
                    inventory.extend(inventory_r)

                except:
                    pass

                # Increase the starting point for next search
                params['start'] += 100

                # Take a rest!
                sleep(self.__sleep_time)

        # Save inventory to raw listings object
        self._inventory_raw = inventory



    # Method to parse existing ticket inventory
    def _parse_inventory(self):

        # Make a dataframe out of listings; drop duplicates
        tickets_df = pd.DataFrame(self._inventory_raw)
        tickets_df = tickets_df.drop_duplicates(subset='listingId')

        # Convert price dictionaries into columns
        tickets_df['price_curr'] = tickets_df.currentPrice.apply(lambda x: x['amount'])
        tickets_df['currency_curr'] = tickets_df.currentPrice.apply(lambda x: x['currency'])
        tickets_df['price_list'] = tickets_df.listingPrice.apply(lambda x: x['amount'])
        tickets_df['currency_list'] = tickets_df.listingPrice.apply(lambda x: x['currency'])

        # Generate delivery type df
        tickets_deliv_type = tickets_df.set_index('listingId')['deliveryTypeList'].\
            apply(pd.Series).stack().reset_index()
        tickets_deliv_type = tickets_deliv_type.drop('level_1', axis=1)
        tickets_deliv_type = tickets_deliv_type.rename(index=str, columns={0: "listings_deliv_type"})

        # Generate delivery method df
        tickets_deliv_method = tickets_df.set_index('listingId')['deliveryMethodList'].\
            apply(pd.Series).stack().reset_index()
        tickets_deliv_method = tickets_deliv_method.drop('level_1', axis=1)
        tickets_deliv_method = tickets_deliv_method.rename(index=str, columns={0: "listings_deliv_method"})

        # Get face value from dict
        tickets_df['faceValue'] = tickets_df.faceValue.apply(lambda x: np.NaN if pd.isnull(x)
                                    else x['amount'])

        # Generate listing attribute df
        ## Use a try statement because sometimes we don't get the listing attr list back from the API
        try:
            tickets_listing_attr = tickets_df.set_index('listingId')['listingAttributeList'].\
                apply(pd.Series).stack().reset_index()
            tickets_listing_attr = tickets_listing_attr.drop('level_1', axis=1)
            tickets_listing_attr = tickets_listing_attr.rename(index=str, columns={0: "listings_listing_attr"})
        except:
            pass

        # Get ticket price from dict
        tickets_df['listingPrice'] = tickets_df.listingPrice.apply(lambda x: x['amount'])

        # Generate ticket splits df
        tickets_splits_df = tickets_df.set_index('listingId')['splitVector'].\
            apply(pd.Series).stack().reset_index()
        tickets_splits_df = tickets_splits_df.drop('level_1', axis=1)
        tickets_splits_df = tickets_splits_df.rename(index=str, columns={0: "tickets_splits_option"})

        # Drop unwanted columns
        tickets_df = tickets_df.drop(['businessGuid', 'currentPrice', 'deliveryMethodList',
                                        'deliveryTypeList', 'listingAttributeList', 'sellerOwnInd',
                                        'splitVector'],
                                        axis = 1)

        # Save ticket dataframes
        self.tickets = {'tickets_df': tickets_df,
                        'tickets_deliv_type': tickets_deliv_type,
                        'tickets_deliv_method': tickets_deliv_method,
                        'tickets_listing_attr': tickets_listing_attr}



    # Wrapper for end-to-end ticket gathering
    def get_tickets(self):

        # Get ticket inventory if we have not yet
        if not self._inventory_raw:
            self._get_inventory()

        # Parse inventory
        self._parse_inventory()

        print('Success!')




scraper = stubhub_scrape(test_mode=False)
scraper.get_tickets()

# Pickle json / dict format in case I want to reprocess later
import pickle
with open('Data/events_json_{}'.format(scraper.scrape_date), 'wb') as file:
    pickle.dump(scraper._events_raw, file)
with open('Data/inventory_json_{}'.format(scraper.scrape_date), 'wb') as file:
    pickle.dump(scraper._inventory_raw, file)

# Save dfs as CSVs
combined_df_dict = {**scraper.events, **scraper.tickets}
for key in combined_df_dict:
    df = combined_df_dict[key]
    file_name = 'Data/tickets_{}_{}.csv'.format(key, scraper.scrape_date)
    df.to_csv(file_name, index=False)
    print('{} written'.format(file_name))
