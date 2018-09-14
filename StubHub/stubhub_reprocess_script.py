#%cd StubHub/
import stubhub_scraper
import pickle
import importlib
_ = importlib.reload(stubhub_scraper)

# Create a list of dates that we've collected; loop through each data and reprocess
dates = ['2018_09_07', '2018_09_08', '2018_09_09', '2018_09_10', '2018_09_11']
for date in dates:

    # Create 'json' file names for each date
    files = ['Data/original_json/events_{}.json'.format(date),
                'Data/original_json/inventory_{}.json'.format(date)]

    # Open pickled 'json' objects
    with open(files[0], 'rb') as events_f:
        events_raw = pickle.load(events_f)
    with open(files[1], 'rb') as tickets_f:
        inventory_raw = pickle.load(tickets_f)

    # Forgot to get the data access time on 9/7
    ## Adding just the date to the dict objects so that processing doesn't break
    if date == '2018_09_07':
        for inv in inventory_raw:
            inv.update({'dt_accessed': date})

    # Set up a scraper object with the given scrape dates and raw json files
    scraper = stubhub_scraper.StubHub_Scrape(scrape_date = date,
                events_raw = events_raw,
                inventory_raw = inventory_raw)

    # Parse events and inventory into dataframes
    scraper._parse_events()
    scraper._parse_inventory()

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
