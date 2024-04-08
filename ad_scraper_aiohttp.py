"""Used for sleep method within scraper"""
import time
import datetime
import asyncio
import os
import logging
import json
from typing import Tuple
import aiohttp
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

now = datetime.datetime.now()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(\
    f'logs/aio_logfile_ad_scraper_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class Limiter:
    '''Rate limiter for aiohttp
    Found on stackoverflow
    '''
    def __init__(self, calls_limit: int = 5, period: int = 1):
        self.calls_limit = calls_limit
        self.period = period
        self.semaphore = asyncio.Semaphore(calls_limit)
        self.requests_finish_time = []

    async def sleep(self):
        if len(self.requests_finish_time) >= self.calls_limit:
            sleep_before = self.requests_finish_time.pop(0)
            if sleep_before >= time.monotonic():
                await asyncio.sleep(sleep_before - time.monotonic())

    def __call__(self, func):
        async def wrapper(*args, **kwargs):

            async with self.semaphore:
                await self.sleep()
                res = await func(*args, **kwargs)
                self.requests_finish_time.append(time.monotonic() + self.period)

            return res

        return wrapper

@Limiter(calls_limit=10, period=1)
async def fetch_url(session: aiohttp.ClientSession, link: str) -> str:
    '''Gets webpage for scrapping
    '''
    async with session.get(link) as response:
        return await response.text()

async def advert_scraper(link: str) -> pd.DataFrame:
    """Scraps adverts for data
    """
    def remote_missing_info(tag):
        """Check if a tag or any of its parents have the 'aria-label' attribute with value 'Obsługa zdalna'.
        """
        for parent in tag.parents:
            if parent.has_attr('aria-label') and parent['aria-label'] == 'Obsługa zdalna':
                return True
        return False

    def custom_filter_label(tag: object) -> bool:
        '''Used for finding chunks with data (labels)
        '''
        if tag.has_attr('data-cy') and tag['data-cy'].startswith('table-label') and tag.get_text() != 'Obsługa zdalna':
            return True
        return False

    def custom_filter_data(tag: object) -> bool:
        '''Used for finding chunks with data (values)
        '''
        if tag.has_attr('data-testid') and tag['data-testid'].startswith('table-value'):
            return True
        if tag.has_attr('data-cy') and tag['data-cy'].startswith('missing') and not remote_missing_info(tag):
            return True
        return False

    def flatten_dict(d:dict)->dict:
        '''Function for dics flattening
        '''
        items = []
        for k, v in d.items():
            if isinstance(v, dict):
                items.extend(flatten_dict(v).items())
            else:
                items.append((k, v))
        return dict(items)

    headers = {'Host': 'host',
               'User-Agent': 'user'}
    try:
        async with aiohttp.ClientSession(headers=headers,connector=aiohttp.TCPConnector(limit=4)) as session:
            link = 'https://www.website.eu' + link
            response = await fetch_url(session, link)
            soup = BeautifulSoup(response, "lxml")
            first_dict = {key.text: value.text for key, value in zip(soup.find_all(custom_filter_label), soup.find_all(custom_filter_data))}
            if 'Obsługa zdalna' in first_dict:
                del first_dict['Obsługa zdalna']
            first_dict['url']=link
            first_dict['Adres'] = soup.find('a', attrs={'aria-label': 'Adres'}).text
            first_dict['Cena'] = soup.find('strong', attrs={'aria-label': 'Cena'}).text
            if soup.find('p', class_=False):
                first_dict['Opis'] = soup.find('p', class_=False).text
            else:
                first_dict['Opis'] = soup.find('div', attrs={'data-cy': "adPageAdDescription"}).text
            second_dict = json.loads(soup.find('script', id="__NEXT_DATA__").text)
            flattened_second_dict = flatten_dict(second_dict)
            keys_to_keep = ['buildId', 'userSessionId', 'ad_photo', 'price_currency',
                            'cat_l1_id', 'cat_l1_name', 'special_offer_type', 'ad_id', 'ad_price',
                            'business', 'city_id', 'city_name', 'poster_type', 'region_id',
                            'region_name', 'subregion_id', 'costs', 'equipment', 'areas',
                            'kitchen', 'parking', 'rooms', 'windowsOrientation', 'numberOfFloors',
                            'security', 'latitude', 'longitude', 'street', 'subdistrict', 'features',
                            'featuresByCategory']

            trimmed_flattened_second_dict = {key: flattened_second_dict[key] for key in keys_to_keep if key in flattened_second_dict}
            data = {**first_dict, **trimmed_flattened_second_dict}
            data = pd.DataFrame.from_dict(data, orient='index').T
            logger.info(link)
            return data
    #I know it is bad. I just do not care here about exceptions very much
    except Exception as e:
        logger.error(e)
        logger.error(link)
        return link

def newest(path: str) -> str:
    '''Finds latest created file with links to scrape'''

    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)

async def main(urls: np.ndarray) -> Tuple[pd.DataFrame, list]:
    '''main function for creating casks and gathering results
    '''
    tasks = [asyncio.create_task(advert_scraper(url)) for url in urls]
    results = await asyncio.gather(*tasks)
    data = [result for result in results if isinstance(result, pd.DataFrame)]
    failed = [result for result in results if not isinstance(result, pd.DataFrame)]
    return data, failed

if __name__ == "__main__":
    failed_links = []
    newest_pickle_file_with_links = newest("./data/links/")
    advert_links = pd.read_pickle(newest_pickle_file_with_links).unique()
    scraped_data_dataframe_list, failed_links = asyncio.run(main(advert_links))
    file_path_data=f'./data/ads/aio_scraped_data_concated_dataframe_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}.json'
    for dataframe in scraped_data_dataframe_list:
        dataframe.to_json(file_path_data,lines=True,mode='a',orient='records')
    pd.to_pickle(failed_links, f'logs/aio_failed_links_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}.pickle')
    df=pd.read_json(f'./data/ads/aio_scraped_data_concated_dataframe_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}.json',lines=True,orient='records')
    df.to_parquet(f'./data/ads/aio_scraped_data_concated_dataframe_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}.parquet')
    logger.info('File saved!')
