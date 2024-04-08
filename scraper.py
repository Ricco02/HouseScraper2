"""Used for sleep methon within scraper"""
import time
import datetime
import os
import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import requests

os.environ['NUMEXPR_MAX_THREADS'] = '11'
os.environ['NUMEXPR_NUM_THREADS'] = '11'
now = datetime.datetime.now()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f'logs/logfile_{now.year}_{now.month}_{now.day}_{now.hour}:{now.minute}.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_max() -> int:
    """Checks how many pages witth adverts there are and returns as int
    """
    headers={'Host': 'host',
             'User-Agent': 'user'}
    response = requests.get('https://www.website.eu/ads/page/last_page_number',
                            headers=headers,
                            timeout=100)
    to_parse = response.content.decode("utf-8")
    soup=BeautifulSoup(to_parse, "lxml")
    max_page:int=int(soup.find_all('li',{'class':"css-1tospdx"})[-1].text)
    return max_page

def generator_of_page_link(max_page_number):
    """Used as generator of llinks for pages with adverts
    """
    for max_page_iter in range(1,max_page_number+1):
        link="https://www.website.eu/ads/page/"+str(max_page_iter)
        yield link

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

async def links_geter(link: str) -> list:
    """Scraps advert listing page for links to adverts
    """
    headers={'Host': 'host',
             'User-Agent': 'user'}
    async with aiohttp.ClientSession(headers=headers,connector=aiohttp.TCPConnector(limit=4)) as session:
        response = await fetch_url(session,link)
        soup=BeautifulSoup(response, "lxml")
        links_found=pd.Series([x for x in [link.get('href') for link in soup.find_all('a')] if '/part/i/do/want' in x and '/part/i/do/not/want' not in x])
        logger.info(link)
        return links_found

async def main(max_page):
    '''Main async function
    '''
    tasks = [asyncio.create_task(links_geter(url)) for url in generator_of_page_link(max_page)]
    results = await asyncio.gather(*tasks)
    return results

if __name__ == "__main__":
    max_number:int=get_max()
    logger.info('Max pages: %s',max_number)
    advert_links=pd.Series()
    advert_links = asyncio.run(main(max_number))
    pd.to_pickle(advert_links,f'data/links/advert_links_{now.year}_{now.month}_{now.day}_{now.hour}')
    logger.info('File saved!')
