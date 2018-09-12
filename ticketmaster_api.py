# Get Ticketmaster Event Listings and Event Prices/Offers

# import libraries
import requests
import pandas as pd
import json
from datetime import datetime
from dateutil.parser import parse

# test event
# migos/drake washington dc 9/12/18
event_id = '150054A9BE5737D6'
# base Commerce API url
base_url = "https://app.ticketmaster.com/commerce/v2/events/{}/offers.json".format(event_id)

# my api key
import re
with open('creds.txt', 'r') as f:
    for line in f:
        if re.match('ticketmaster', line):
            my_key = line.strip().replace(" ", "").split(':')[1]

my_key


# let's build a class to get current ticket offers on ticketmaster per event
# use ticketmaster's Commerce API
class ticketmasterEvent(object):

    """ This object makes an api request to get a specific event's ticket offers
        The getPrices method returns a json of the event's offered ticket prices """

    def __init__(self, eventId, apikey = my_key):
        # import libraries
        import pandas as pd
        import requests

        self.eventId = eventId
        # get response from API
        self.url = "https://app.ticketmaster.com/commerce/v2/events/{}/offers.json".format(self.eventId)
        self.key = apikey
        self.response = requests.get(self.url, params={'apikey': self.key})
        self.data = self.response.json()


    # get prices
    def getPrices(self):
        # columns to be filled in dataframe
        columns = ('TMEventId', 'TicketId', 'TicketType', 'PriceZone', 'ListedPrice', 'TotalPrice')
        # columns for areas dataframe
        area_columns = ('TMEventId', 'AreaName', 'AreaDesc', 'AreaRank', 'PriceZone')

        # initialize list of areas
        self._areas = []

        # area info
        if 'areas' in self.data['_embedded'].keys():
            for area in self.data['_embedded']['areas']['data']:
                # TMEventId
                event_id = self.data['metadata']['eventMapping']['source']['id']
                # AreaName
                name = [area['attributes']['name'] if 'name' in area['attributes'].keys() else "NA"][0]
                # AreaDesc
                desc = [area['attributes']['description'] if 'description' in area['attributes'].keys() else "NA"][0]
                # AreaRank
                rank = [area['attributes']['rank'] if 'rank' in area['attributes'].keys() else "NA"][0]
                # priceZone
                if ('relationships' in area.keys() and 'priceZones' in area['relationships'].keys()):
                    for zone in area['relationships']['priceZones']['data']:
                        temp = [event_id, name, desc, rank]
                        temp.append(zone['id'])
                        self._areas.append(tuple(temp))
                else: self._areas.append(tuple([event_id, name, desc, rank, "NA"]))


        # define empty lists to fill with ticket/area info
        self._data = []

        offers = self.data['offers']

        for offer in offers:
            # get ticket id and type
            event_id = self.data['metadata']['eventMapping']['source']['id']
            ticket_id = [offer['id'] if 'id' in offer.keys() else "NA"][0]
            ticket_type = [offer['attributes']['description'] if 'description' in offer['attributes'].keys() else "NA"][0]
            # get attributes/prices for each price zone
            if 'prices' in offer['attributes'].keys():
                for zone in offer['attributes']['prices']:
                    price_zone = zone['priceZone']
                    listed_price = zone['value']
                    total_price = zone['total']
                    # append to list
                    self._data.append((event_id, ticket_id, ticket_type, price_zone, listed_price, total_price))
            else:
                self._data.append((event_id, ticket_id, ticket_type, "NA", "NA", "NA"))

        # make into dataframe
        self.prices_df = pd.DataFrame(self._data, columns = columns)
        self.areas_df = pd.DataFrame(self._areas, columns = area_columns)
        return self.prices_df, self.areas_df



# build another class to get event list for a city
# use ticketmaster's Discovery API
class ticketmasterEventListings(object):
    """ This class makes an api request to get MUSIC events in one locale """

    def __init__(self, startDate, city, apikey = my_key):
        # import libraries
        import pandas as pd
        import requests
        from datetime import datetime
        from dateutil.parser import parse
        import json

        #self.dmaId = dmaId
        self.startDate = parse(startDate).strftime("%Y-%m-%dT%H:%M:%SZ")
        # if want to imput endDate, add to __init__ args, and add to self.params 'endDateTime':
        #self.endDate = parse(endDate).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.city = city

        # get response from API
        self.url = "https://app.ticketmaster.com/discovery/v2/events.json"
        self.key = apikey
        self.params = {'apikey': my_key, 'startDateTime': self.startDate, 'countryCode': 'US',
        'classificationId': 'KZFzniwnSyZfZ7v7nJ', 'city': self.city, 'size': 200, 'page': 0}
        self.response = requests.get(self.url, params=self.params)
        self.data = json.loads(self.response.text)
        if len(self.data) > 0:
            print("Got page 0")
            # get how many pages need to be requested
            self.pages = self.data['page']['totalPages']
            self.elements = self.data['page']['totalElements']
            print("Total Pages: {}".format(self.pages))
            print("Total Elements: {}".format(self.elements))

            # append all events
            # cannot have page * size > 1000
            for i in range(1, 5):
                if i in range(1, self.pages):
                    self.params['page'] = i
                    _response = requests.get(self.url, params=self.params)
                    self._temp_data = json.loads(_response.text)
                    print("Got page {}".format(i))
                    print(self._temp_data.keys())
                    for event in self._temp_data['_embedded']['events']:
                        self.data['_embedded']['events'].append(event)

                    print("Appended page {}".format(i))
                    print("Number of events: {}".format(len(self.data['_embedded']['events'])))
                    print("="* 50)


    # get details of all the events
    def getDetails(self):
        # columns
        columns = ['TMEventId', 'City', 'State', 'EventName', 'DateTime', 'LocalDate', 'LocalTime', 'TimeZone',
        'EventGenre', 'Venue', 'Address', 'Zipcode', 'Latitude', 'Longitude', 'MinPrice',
        'MaxPrice', 'Promoter', 'PublicSaleStart', 'PublicSaleEnd', 'EventURL']
        # presales
        columns_presale = ['TMEventId', 'PresaleName', 'PresaleStart', 'PresaleEnd']
        # Artists
        columns_artists = ['TMEventId', 'Artist', 'Rank', 'UpcomingEvents']

        # empty list to append values to
        self._data = []
        self._presales = []
        self._artists = []

        # loop through all events
        #for event in random.sample(self.data['_embedded']['events'],10):
        for event in self.data['_embedded']['events']:
            # empty list of values
            temp_values = []
            #temp_presales = []
            #temp_artists = []
            temp_keys = list(event.keys())

            # TMEventId
            temp_values.append(event['id'])
            # dmaId
            #temp_values.append(self.dmaId)
            # City
            temp_values.append(event['_embedded']['venues'][0]['city']['name'])
            # State
            temp_values.append(event['_embedded']['venues'][0]['state']['stateCode'])
            # EventName
            temp_values.append([event['name'] if 'name' in temp_keys else 'NA'][0])
            # DateTime
            temp_values.append([event['dates']['start']['dateTime'] if ('dateTime' in event['dates']['start'].keys() and (event['dates']['start']['dateTBA'] == False or event['dates']['start']['dateTBD'] == False)) else 'TBA'][0])
            # LocalDate
            temp_values.append([event['dates']['start']['localDate'] if ('localDate' in event['dates']['start'].keys() and (event['dates']['start']['dateTBA'] == False or event['dates']['start']['dateTBD'] == False)) else 'TBA'][0])
            # LocalTime
            temp_values.append([event['dates']['start']['localTime'] if ('localTime'in event['dates']['start'].keys() and (event['dates']['start']['noSpecificTime'] == False or event['dates']['start']['timeTBA'] == False)) else 'TBA'][0])
            # TimeZone
            temp_values.append([event['dates']['timezone'] if 'timezone' in list(event['dates'].keys()) else 'NA'][0])
            # EventGenre
            temp_values.append(event['classifications'][0]['genre']['name'])

            # check for listed Artists to put in artists table
            if 'attractions' in event['_embedded'].keys():
                # Artists
                i = 1
                for artist in event['_embedded']['attractions']:
                    temp = [event['id']]
                    # TMEventId, Artist, Rank, UpcomingEvents
                    temp.append([artist['name'] if 'name' in artist.keys() else "NA"][0])
                    temp.append(i)
                    temp.append([artist['upcomingEvents']['_total'] if ('upcomingEvents' in artist.keys() and '_total' in artist['upcomingEvents'].keys()) else "NA"][0])
                    self._artists.append(tuple(temp))
                    i += 1

            # Venue
            temp_values.append(event['_embedded']['venues'][0]['name'])
            # check if address
            if 'address' in event['_embedded']['venues'][0].keys():
                # Address
                temp_values.append([event['_embedded']['venues'][0]['address']['line1'] if 'line1' in event['_embedded']['venues'][0]['address'].keys() else "NA"][0])
            else:
                temp_values.append("NA")
            # ZipCode
            temp_values.append([event['_embedded']['venues'][0]['postalCode'] if 'postalCode' in event['_embedded']['venues'][0].keys() else "NA"][0])
            # Latitude
            temp_values.append([event['_embedded']['venues'][0]['location']['latitude'] if 'location' in event['_embedded']['venues'][0].keys() else "NA"][0])
            # Longitude
            temp_values.append([event['_embedded']['venues'][0]['location']['longitude'] if 'location' in event['_embedded']['venues'][0].keys() else "NA"][0])
            # MinPrice
            temp_values.append([event['priceRanges'][0]['min'] if 'priceRanges' in temp_keys else 'NA'][0])
            # MaxPrice
            temp_values.append([event['priceRanges'][0]['max'] if 'priceRanges' in temp_keys else 'NA'][0])
            # Promoter
            temp_values.append([event['promoters'][0]['name'] if 'promoters' in temp_keys else event['promoter']['name'] if 'promoter' in temp_keys else 'NA'][0])

            # check if there is a sales key.
            if 'sales' in temp_keys:
                # add presales to presales list
                if 'presales' in event['sales'].keys():
                    for presale in event['sales']['presales']:
                        temp = [event['id']]
                        # TMEventId, PresaleName PresaleStart, PresaleEnd
                        temp.append([presale['name'] if 'name' in presale.keys() else "NA"][0])
                        temp.append([presale['startDateTime'] if 'startDateTime' in presale.keys() else "NA"][0])
                        temp.append([presale['endDateTime'] if 'endDateTime' in presale.keys() else "NA"][0])
                        self._presales.append(tuple(temp))
                # PublicSaleStart
                temp_values.append([event['sales']['public']['startDateTime'] if 'startDateTime' in list(event['sales']['public'].keys())  else 'NA'][0])
                # PublicSaleEnd
                temp_values.append([event['sales']['public']['endDateTime'] if 'startDateTime' in list(event['sales']['public'].keys())  else 'NA'][0])
            else:
                # fill NA for no sales listed
                temp_values.append("NA")
                temp_values.append("NA")

            # EventURL
            temp_values.append([event['url'] if 'url' in temp_keys else 'NA'][0])

            # append to _data
            self._data.append(tuple(temp_values))



        #self.details_list = [dict(list(zip(columns, i))) for i in self._data]
        self.details_df = pd.DataFrame(self._data, columns = columns)
        self.presales_df = pd.DataFrame(self._presales, columns = columns_presale)
        self.artists_df = pd.DataFrame(self._artists, columns = columns_artists)
        self.endDateTime = self.details_df[self.details_df['DateTime'] != 'TBA']['DateTime'].max()
        #print(self.endDateTime)
        return self.details_df, self.presales_df, self.artists_df



music_class_id = 'KZFzniwnSyZfZ7v7nJ'
dma_ids = {249: 'Chicago', 324: 'Los Angeles', 345: 'New York', 382: 'San Francisco - Oakland - San Jose', 409: 'Washington DC', 235: 'Boston'}

cities = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose', 'New York', 'Brooklyn', 'Bronx', 'Flushing', 'East Rutherford',
'Washington, DC', 'Vienna', 'Chicago', 'Rosemont', 'Evanston', 'Los Angeles', 'Hollywood', 'West Hollywood', 'Pasadena',
'Boston', 'Medford', 'Cambridge']

# collect data - test
#temp_events = ticketmasterEventListings(startDate='9/9/18', city='Brooklyn')
#temp_data = temp_events.getDetails()

columns = ['TMEventId', 'City', 'State', 'EventName', 'DateTime', 'LocalDate', 'LocalTime', 'TimeZone',
'EventGenre', 'Venue', 'Address', 'Zipcode', 'Latitude', 'Longitude', 'MinPrice',
'MaxPrice', 'Promoter', 'PublicSaleStart', 'PublicSaleEnd', 'EventURL']
# presales
columns_presale = ['TMEventId', 'PresaleName', 'PresaleStart', 'PresaleEnd']
# Artists
columns_artists = ['TMEventId', 'Artist', 'Rank', 'UpcomingEvents']
# initialize empty dataframes
events = pd.DataFrame(columns = columns)
presale = pd.DataFrame(columns = columns_presale)
artists = pd.DataFrame(columns = columns_artists)

# get event details for all cities and concatenate the data frames
for city in cities:
    temp_events = ticketmasterEventListings(startDate='9/9/18', city=city)
    temp_data = temp_events.getDetails()
    events = pd.concat([events, temp_data[0]], ignore_index=True)
    presale = pd.concat([presale, temp_data[1]], ignore_index=True)
    artists = pd.concat([artists, temp_data[2]], ignore_index=True)
    print("{} done".format(city))
    print("="*50)
    print("="*50)

# send to csv
events.to_csv("TM_EventDetails.csv")
presale.to_csv("TM_PresalesDetails.csv")
artists.to_csv("TM_ArtistDetails.csv")



# get dataframes of prices and areas
price_columns = ('TMEventId', 'TicketId', 'TicketType', 'PriceZone', 'ListedPrice', 'TotalPrice')
# columns for areas dataframe
area_columns = ('TMEventId', 'AreaName', 'AreaDesc', 'AreaRank', 'PriceZone')
# initialize empty dataframes
prices = pd.DataFrame(columns = price_columns)
areas = pd.DataFrame(columns = area_columns)

# get prices for events that have prices
# need to limit to 3000 for day 1, bc a little over 5000 events total, but 5000 api calls/day allowed
i = 0
for id in events['TMEventId'].iloc[4900:]:
    print(id)
    print(i)
    temp_event = ticketmasterEvent(id)
    if len(temp_event.data) > 1:
        temp_data = temp_event.getPrices()
        prices = pd.concat([prices, temp_data[0]], ignore_index=True)
        areas = pd.concat([areas, temp_data[1]], ignore_index=True)
        print("Added Prices")
        print("="*50)
    else:
        print("No Available Prices")
        print("="*50)
    i += 1

# last one to date: 4899
prices
# send to csv
prices.to_csv("TM_Prices.csv")
areas.to_csv("TM_PriceAreas.csv")



# test
events_url = 'https://app.ticketmaster.com/discovery/v2/events.json'

# classificationId for music
# dmaId for locale
event_search_params = {'city': 'Cambridge', 'apikey': my_key, 'startDateTime': '2018-09-09T00:00:00Z', 'countryCode': 'US', 'classificationId': 'KZFzniwnSyZfZ7v7nJ', 'size': 20, 'page': 0}

response = requests.get(events_url, params=event_search_params)

# check how many calls left
response.headers

# New York: 1348
# Brooklyn: 513
# Bronx: 1
# Flushing: 2
# East Rutherford; 2
# San Francisco: 560
# Oakland: 104
# Berkeley: 41
# San Jose: 20
# Washington DC: 5
# Vienna: 13
# Chicago: 1031
# Rosemont: 63
# Evanston: 3
# Los Angeles: 668
# Hollywood: 308
# West Hollywood: 144
# Pasadena: 41
# Boston: 468
# Medford: 21
# Cambridge: 299
