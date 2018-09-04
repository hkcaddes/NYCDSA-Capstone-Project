https://app.ticketmaster.com/commerce/v2/events/150054A9BE5737D6/offers.json?apikey=qFpjKZzH4tImeE5tfDYeGvx0DrzybOib

# test ticketmaster's event offers (commerce) api
base_url = "https://app.ticketmaster.com/commerce/v2/events/{}/offers.json".format(event_id)
# event id
# migos/drake washington dc 9/12/18
event_id = '150054A9BE5737D6'

# my api key
my_key = 'qFpjKZzH4tImeE5tfDYeGvx0DrzybOib'



# let's build a class
class ticketmasterEvent(object):

    """docstring for ."""

    import pandas as pd
    import requests


    def __init__(self, eventId, apikey = 'qFpjKZzH4tImeE5tfDYeGvx0DrzybOib'):
        self.eventId = eventId
        # get response from API
        self.url = "https://app.ticketmaster.com/commerce/v2/events/{}/offers.json".format(self.eventId)
        self.key = apikey
        self.response = requests.get(self.url, params={'apikey': self.key})
        self.data = self.response.json()


    # get prices
    def getPrices(self):
        # columns to be filled in dataframe
        columns = ('TicketId', 'TicketType', 'PriceZone', 'Areas', 'ListedPrice', 'TotalPrice')

        offers = self.data['offers']

        # define empty list to fill with each ticket's info
        self._data = []

        for offer in offers:
            # get ticket id and type
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
                self._data.append((ticket_id, ticket_type, price_zone, areas_in_pricezone, listed_price, total_price))
                i += 1

        self.prices_df = pd.DataFrame(self._data, columns = columns)
        return self.prices_df



# test class method
event = ticketmasterEvent(event_id)

# test getPrices
event.getPrices()


response = requests.get(base_url, params = {'source': 'TMR', 'apikey': my_key})
response.url
response.json()
