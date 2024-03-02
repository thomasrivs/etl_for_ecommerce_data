import time

from etl.extract import get_data
from etl.load import load_data_into_gcs
from config import COUNTRIES
import logging
from datetime import datetime


# Configure logging
logging.basicConfig(filename='info.log', level=logging.INFO)

start_date = '2023-12-01-01'
end_date = '2023-12-31-23'

for country in COUNTRIES:
    try:
        rows = get_data(country, start_date, end_date)
        load_data_into_gcs(rows,f'raw/{country}')
        logging.info(f'{datetime.now()}: loaded {len(rows)} rows in raw/{country} blob')
    except Exception as e:
        logging.error(f'Error while loading rows for {country}: {e}')

    time.sleep(5)





