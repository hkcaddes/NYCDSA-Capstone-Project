# import required packages

import requests
import json
import base64
import pandas as pd
import re
from time import sleep
import datetime


class stubhub_scrape(object):

    # Initialize class
    def __init__(self, sleep_time=6, test_mode = False):

        # Set the amount of time to sleep after each call
        self.__sleep_time = sleep_time
        self.__test_mode = test_mode


        # Store the access token... initialize as none
        self.__headers = None
        self._gen_auth_header_()

        # Save the events dataframe... initialize as none

        self.events = None

        # Save the listings dataframe... initialize as none
        self._listings_raw = None
        self.listings = None


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



    # Method to generate a list of events
    def get_events(self):

        # Define events url and query params
        events_url = 'https://api.stubhub.com/search/catalog/events/v3'
        params = {'city': self.__city_list, 'q': 'concert',
         'start': 0, 'rows': 500, 'fieldList': '*,ticketInfo'}

        # Run the first request; get the total number of events
        events_r = requests.get(events_url, headers = self.__headers, params = params).json()
        n_found = events_r['numFound']

        # Start collecting the event response objects; mark with the datetime collected
        events =  events_r['events']
        list(map(lambda i: i.update({'dt_accessed': str(datetime.datetime.now())}), events))


        # Take a rest!
        sleep(self.__sleep_time)

        # Run a loop through search results and collect
        while params['start'] < n_found:

            # Get results, mark with dt, and store
            events_r = requests.get(events_url, headers = self.__headers, params = params).json()['events']
            list(map(lambda i: i.update({'dt_accessed': str(datetime.datetime.now())}), events_r))
            events.extend(events_r)

            # Increment through pages; sleep before moving on
            params['start'] += 500
            sleep(self.__sleep_time)

            # Escape the while loop after two calls if in test mode
            if self.__test_mode and params['start']>1000:
                break




        # Turn events list into a dataframe and remove duplicate events
        events_df = pd.DataFrame(events)
        events_df = events_df.drop_duplicates(subset='id')


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



        # Generate venues DataFrame
        venues = events_df.venue.apply(pd.Series)
        venues = venues.drop_duplicates(subset = 'id')
        venues = venues.rename(index=str, columns={'id': 'venue_id'})

        # Label events dataframe with venue id and drop venue from events df
        events_df['venue_id'] = events_df.venue.apply(lambda i: i['id'])

        # Drop unwanted columns
        events_df = events_df.drop(['ancestors', 'associatedEvents', 'bobId',
                                    'catalogTemplate', 'categories', 'categoriesCollection',
                                    'displayAttributes','eventAttributes', 'groupings',
                                    'groupingsCollection', 'imageUrl', 'locale', 'mobileAttributes',
                                    'performers', 'performersCollection', 'sourceId',
                                    'venue'],
                                     axis = 1)

        events_df = events_df.rename(index=str, columns={'id': 'event_id'})


        self.events = {'events_df': events_df, 'events_perf': events_perf}


    # Method to gather ticket inventory (json objects)
    def _get_inventory(self):

        # Get events if we have not yet
        if not self.events:
            self.get_events()

        # Get a list of event IDs that have listings
        self.events['events_df']['n_tickets'] = self.events['events_df'].ticketInfo.apply(
                                    lambda x: x['totalListings'])
        events_l = self.events['events_df'].loc[self.events['events_df'].n_tickets>0, ['event_id', 'n_tickets']]
        events_l = events_l.reset_index()
        print(events_l.head())

        # Save inventory URL
        inventory_url = 'https://api.stubhub.com/search/inventory/v2'

        # Initialize empty inventory list
        inventory = []

        # Only keep first 5 listings if we're in test mode
        if self.__test_mode:
            n = 5 # First n listings for testing
            events_l = events_l.iloc[0:n, :]

        # Loop through each event and get ticket listings
        for i, id, n_tickets in events_l.itertuples():

            print('starting event {}, {}, {}'.format(i, id, n_tickets))

            # Initialize parameters and blank inventory list
            params = {'eventid': id, 'start': 0, 'rows': 250}


            # Continue looping until we've gotten all of the listings
            while params['start'] <= n_tickets:

                # Query for listings
                inventory_r = requests.get(inventory_url, headers=self.__headers,
                    params=params).json()['listing']
                print(len(inventory_r))

                # Add the event id to each listing
                list(map(lambda i: i.update({'event_id': id}), inventory_r))
                print(len(inventory_r))

                # Add responses to the inventory list
                inventory.extend(inventory_r)
                print(len(inventory))

                # Increase the starting point for next search
                params['start'] += 100

                # Take a rest!
                sleep(self.__sleep_time)

            print('finished event {}'.format(i))

        # Save inventory to raw listings object
        self._inventory_raw = inventory



    # Method to parse existing ticket inventory
    def _parse_inventory(self):

        # Make a dataframe out of listings; drop duplicates
        listings_df = pd.DataFrame(self._inventory_raw)
        listings_df = listings_df.drop_duplicates(subset='listingId')

        # Convert price dictionaries into columns
        listings_df['price_curr'] = listings_df.currentPrice.apply(lambda x: x['amount'])
        listings_df['currency_curr'] = listings_df.currentPrice.apply(lambda x: x['currency'])
        listings_df['price_list'] = listings_df.listingPrice.apply(lambda x: x['amount'])
        listings_df['currency_list'] = listings_df.listingPrice.apply(lambda x: x['currency'])
        listings_df = listings_df.drop('currentPrice', axis=1)

        # Generate delivery option df
        listings_deliv_type = listings_df.set_index('listingId')['deliveryTypeList'].\
            apply(pd.Series).stack().reset_index()
        listings_deliv_type = listings_deliv_type.drop('level_1', axis=1)
        listings_deliv_type = listings_deliv_type.rename(index=str, columns={0: "listings_deliv_type"})


        self.tickets = {'tickets_df': listings_df,
                        'tickets_deliv_type': listings_deliv_type}



    # Wrapper for end-to-end ticket gathering
    def get_tickets(self):

        if not self._listings_raw:
            self._get_inventory()

        self._parse_inventory()

        print('Success!')











scraper = stubhub_scrape(test_mode=True)
scraper.get_tickets()

scraper.tickets.keys()

len(scraper.listings['listings_df'])

import pickle
with open('Data/listings_json_2018_09_07', 'wb') as file:
    pickle.dump(scraper.listings['raw'], file)

with open('Data/events_json_2018_09_07', 'wb') as file:
    pickle.dump(scraper.events['raw'], file)

scraper.events.keys()
scraper.events['events_perf'].to_csv('Data/events_perf_2018_09_07.csv')

scraper.listings['listings_df'].dt_accessed
