#%cd StubHub/
#from stubhub_scraper import StubHub_Scrape
import stubhub_scraper
import pandas as pd
import importlib

_ = importlib.reload(stubhub_scraper)

scraper = stubhub_scraper.StubHub_Scrape(test_mode = True,
                                            events_raw = events_raw,
                                            inventory_raw = inventory_raw)
scraper._parse_inventory()

scraper.tickets.keys()
scraper.tickets['tickets_splits']

inventory_raw

scraper.tickets[]

scraper.get_events()
events_raw = scraper._events_raw
inventory_raw = scraper._inventory_raw
