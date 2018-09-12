#%cd StubHub/
from stubhub_scraper import StubHub_Scrape
import pandas as pd
import pickle
import importlib
_ = importlib.reload(stubhub_scraper)

scraper = StubHub_Scrape(test_mode=False)
scraper.get_tickets()

# Pickle json / dict format in case I want to reprocess later
with open('Data/events_{}.json'.format(scraper.scrape_date), 'wb') as file:
    pickle.dump(scraper._events_raw, file)
with open('Data/inventory_{}.json'.format(scraper.scrape_date), 'wb') as file:
    pickle.dump(scraper._inventory_raw, file)

# Save dfs as CSVs
combined_df_dict = {**scraper.events, **scraper.tickets}
for key in combined_df_dict:
    df = combined_df_dict[key]
    key_splits = key.split('_')
    schema = key_splits[0]
    table_name = '_'.join(key_splits[1:])
    file_name = 'Data/{}/{}/{}_{}_{}.csv'.format(schema, table_name, schema,
                    table_name, scraper.scrape_date)
    df.to_csv(file_name, index=False)
    print('{} written'.format(file_name))
