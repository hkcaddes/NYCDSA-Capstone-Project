import requests
import json
import pandas as pd
pd.set_option('display.max_columns', 500)

responseEvents = requests.get("https://api.seatgeek.com/events?per_page=5000&client_id=MTI5NzU0MTd8MTUzNTkwMTU5Mi45Nw")

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

df = pd.DataFrame({"local_date": local_date, "utc_date": utc_date, "Id": Id, "score": score, "title":title, 
                   "avgPrice": avgPrice, "hiPrice": hiPrice, "listCount": listCount, "lowPrice": lowPrice, 
                   "lowPriceDeals": lowPriceDeals, "Type":Type, "city": city, "state":state, 
                   "lat": lat, "lon": lon, "postal":postal, "url": url, "performer": performer})





