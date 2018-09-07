# test ticketmaster's event offers (commerce) api
# event id
# migos/drake washington dc 9/12/18
event_id = '150054A9BE5737D6'
base_url = "https://app.ticketmaster.com/commerce/v2/events/{}/offers.json".format(event_id)

# my api key
my_key = 'XXXXXXXXXXXXXXXXXXXXXXXX'

import requests
import pandas as pd
import json
from datetime import datetime
from dateutil.parser import parse

# let's build a class
class ticketmasterEvent(object):

    """ This object makes an api request to get a specific event's ticket offers
        The getPrices method returns a json of the event's offered ticket prices """

    def __init__(self, eventId, apikey = 'XXXXXXXXXXXXXXXXXXXXXXXX'):
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
        columns = ('TMEventId', 'TicketId', 'TicketType', 'PriceZone', 'Areas', 'ListedPrice', 'TotalPrice')

        # dictionary of area ids and their descriptions
        self.areas = {area['id']: area['attributes']['description'] for area in self.data['_embedded']['areas']['data']}

        offers = self.data['offers']

        # define empty list to fill with each ticket's info
        self._data = []

        for offer in offers:
            # get ticket id and type
            event_id = self.data['metadata']['eventMapping']['source']['id']
            ticket_id = offer['id']
            ticket_type = offer['attributes']['description']
            i = 0

            # get attributes/prices for each price zone
            for zone in offer['attributes']['prices']:
                price_zone = zone['priceZone']
                listed_price = zone['value']
                total_price = zone['total']

                # get areas that are in the given price zone
                areas = self.data['_embedded']['priceZones']['data'][i]['relationships']['areas']['data']
                areas_in_pricezone = [area['id'] for area in areas]
                areas_in_pricezone = {int(id): desc for id, desc in list(self.areas.items()) if id in str(areas_in_pricezone)}

                # make into dictionary to make into kind of a json
                #values = [event_id, ticket_id, ticket_type, price_zone, areas_in_pricezone, listed_price, total_price]
                #temp_data = dict(zip(columns, values))
                # append to list
                self._data.append((event_id, ticket_id, ticket_type, price_zone, areas_in_pricezone, listed_price, total_price))
                i += 1

        # make into dataframe
        self.prices_df = pd.DataFrame(self._data, columns = columns)
        return self.prices_df




# test class method
event1 = ticketmasterEvent(event_id)
x = event1.getPrices()
x

# build another class to get event list for a dmaId
class ticketmasterEventListings(object):
    """ This class makes an api request to get MUSIC events in one locale """

    def __init__(self, dmaId, startDate, endDate, apikey = 'XXXXXXXXXXXXXXXXXXXXXXXX'):
        # import libraries
        import pandas as pd
        import requests
        from datetime import datetime
        from dateutil.parser import parse
        import json

        self.dmaId = dmaId
        self.startDate = parse(startDate).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.endDate = parse(endDate).strftime("%Y-%m-%dT%H:%M:%SZ")

        # get response from API
        self.url = "https://app.ticketmaster.com/discovery/v2/events.json"
        self.key = apikey
        self.params = {'apikey': my_key, 'startDateTime': self.startDate, 'endDateTime': self.endDate, 'countryCode': 'US',
        'classificationId': 'KZFzniwnSyZfZ7v7nJ', 'dmaId': self.dmaId, 'size': 200, 'page': 0}
        self.response = requests.get(self.url, params=self.params)
        self.data = json.loads(self.response.text)
        if len(self.data) > 1:
            print("Got page 0")
            # get how many pages need to be requested
            self.pages = self.data['page']['totalPages']

            # append all events
            #for i in range(1, 3):
            for i in range(1, self.pages):
                self.params['page'] = i
                _response = requests.get(self.url, params=self.params)
                _temp_data = json.loads(_response.text)
                print("Got page {}".format(i))
                print(_temp_data.keys())
                for event in _temp_data['_embedded']['events']:
                    self.data['_embedded']['events'].append(event)

                print("Appended page {}".format(i))
                print("Number of events: {}".format(len(self.data['_embedded']['events'])))
                print("="* 50)

    # get details of all the events
    def getDetails(self):
        # columns
        columns = ['TMEventId', 'DMAId', 'City', 'State', 'EventName', 'DateTime', 'LocalDate', 'LocalTime', 'TimeZone',
        'EventGenre', 'Artists', 'ArtistsUpcoming', 'Venue', 'Address', 'Zipcode', 'Latitude', 'Longitude', 'MinPrice',
        'MaxPrice', 'Promoter', 'PresaleStart', 'PresaleEnd', 'PublicSaleStart', 'PublicSaleEnd', 'EventURL']
        # empty list to append values to
        self._data = []

        # loop through all events
        #for event in random.sample(self.data['_embedded']['events'],10):
        for event in self.data['_embedded']['events']:
            # empty list of values
            temp_values = []
            temp_keys = list(event.keys())

            # TMEventId
            temp_values.append(event['id'])
            # dmaId
            temp_values.append(self.dmaId)
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
            # check for listed Artists
            if 'attractions' in event['_embedded'].keys():
                # Artists
                temp_values.append({i: event['_embedded']['attractions'][i]['name'] for i in range(len(event['_embedded']['attractions']))})
                # ArtistsUpcoming
                temp_values.append({i: event['_embedded']['attractions'][i]['upcomingEvents']['_total'] for i in range(len(event['_embedded']['attractions']))})
            else:
                temp_values.append({"NA": "NA"})
                temp_values.append({"NA": "NA"})
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
            # check if there is a presale.
            if 'sales' in temp_keys:
                if 'presales' in event['sales'].keys():
                    # PresaleStart
                    temp_values.append({event['sales']['presales'][i]['name']: event['sales']['presales'][i]['startDateTime'] for i in range(len(event['sales']['presales']))})
                    # PresaleEnd
                    temp_values.append({event['sales']['presales'][i]['name']: event['sales']['presales'][i]['startDateTime'] for i in range(len(event['sales']['presales']))})
                else:
                    # fill NA if no presale
                    temp_values.append({"NA": "NA"})
                    temp_values.append({"NA": "NA"})
                # PublicSaleStart
                temp_values.append([event['sales']['public']['startDateTime'] if 'startDateTime' in list(event['sales']['public'].keys())  else 'NA'][0])
                # PublicSaleEnd
                temp_values.append([event['sales']['public']['endDateTime'] if 'startDateTime' in list(event['sales']['public'].keys())  else 'NA'][0])
            else:
                # fill NA for no sales listed
                temp_values.append({"NA": "NA"})
                temp_values.append({"NA": "NA"})
                temp_values.append({"NA": "NA"})
                temp_values.append({"NA": "NA"})

            # EventURL
            temp_values.append([event['url'] if 'url' in temp_keys else 'NA'][0])

            # append to _data
            self._data.append(tuple(temp_values))

        #self.details_list = [dict(list(zip(columns, i))) for i in self._data]
        self.details_df = pd.DataFrame(self._data, columns = columns)
        return self.details_df



ny2 = ticketmasterEventListings(345, '9/7/18', '9/30/18')
ny2.getDetails()



music_class_id = 'KZFzniwnSyZfZ7v7nJ'
dma_ids = {249: 'Chicago', 324: 'Los Angeles', 345: 'New York', 382: 'San Francisco - Oakland - San Jose', 409: 'Washington DC'}

events_url = 'https://app.ticketmaster.com/discovery/v2/events.json'

# classificationId for music
# dmaId for locale
event_search_params = {'apikey': my_key, 'startDateTime': '2018-09-05T00:00:00Z', 'endDateTime': '2018-09-30T00:00:00Z', 'countryCode': 'US', 'classificationId': 'KZFzniwnSyZfZ7v7nJ', 'dmaId': 345}

response = requests.get(events_url, params=event_search_params)

data = json.loads(response.text)

event = data['_embedded']['events'][13]

[event['sales']['public']['startDateTime'] if 'startDateTime' in list(event['sales']['public'].keys())  else 'NA'][0]
