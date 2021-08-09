# -*- coding: utf-8 -*-

import logging
import datetime
from time import strptime
import json
import urllib.parse as urlparse
import re
import lxml

from scrapy import Request
from bs4 import BeautifulSoup
import cfscrape

from fourgol.fourgol_spider import FourgolSpider
from fourgol.settings import AWS_DYNAMODB_TABLE_NAME
from fourgol.items import Thread

logger = logging.getLogger('cracked')


class CrackedSpider(FourgolSpider):
    name = 'cracked'
    allowed_domains = ['cracked.to']
    custom_settings = {
        'AWS_DYNAMODB_TABLE_NAME' : AWS_DYNAMODB_TABLE_NAME % name
    }

    # crawlera_enabled = True
    # crawlera_apikey = ''

    def __init__(self, name=None, **kwargs):
        self.base_url = 'https://cracked.to/'
        self.default_page = 1
        self.request_header = {
	        "Accept": "*/*",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4'
        }
        self.scraper = cfscrape.CloudflareScraper()

        logger.debug("%s spider was initialized." % self.name)
        pass

    def start_requests(self): # Register the Callback Function on each categories to [crawl whole Board]
        category_dict = self.__generate_category_dict()
        logger.info('__generate_category_dict %s' % json.dumps(category_dict, ensure_ascii=False))

        meta = dict()
        meta['page'] = self.default_page
        meta['category'] = list()
        meta['depth'] = 1

        return [Request(url=x['response_url'] + '?page=1', callback=self.parse, headers=self.request_header, meta=x)
                for x in self.get_categories(category_dict, meta=meta)] # Return the List of scrapy.Request

    def __generate_category_dict(self):
        category_dict = dict()
        r = self.scraper.get(self.base_url)
        bs_obj = BeautifulSoup(r.text, 'lxml')

        section_container = bs_obj.select('div.content > table')
        for section in section_container:
            section_name = section.select_one('thead').text.strip()
            category_dict[section_name] = dict()
            sub_section_container = section.select('tbody > tr')[1:]

            for sub_section in sub_section_container:
                if not sub_section.has_attr('class'):
                    sub_forums = sub_section.select('a')
                    for forum in sub_forums:
                        forum_name = forum.text.strip()
                        category_dict[section_name][forum_name] = forum['href']
                else:
                    forum_name = sub_section.select('td')[1].select_one('strong').text.strip()
                    forum_url = sub_section.select('td')[1].select_one('strong').select_one('a')['href']
                    category_dict[section_name][forum_name] = forum_url

        categories_to_remove = [
            ['Cracked.to related'],
            ['Hacking & Exploits'],
            ['Banter', 'Lounge'],
            ['Banter', 'Innuendo'],
            ['Banter', 'Entertainment'],
            ['Banter', 'Music'],
            ['Banter', 'Introduction'],
            ['Banter', 'LQ Lounge'],
            ['Banter', 'Gaming'],
            ['Banter', 'First Person Shooters'],
            ['Banter', 'Fortnite'],
            ['Banter', 'RPG/MMPORG games'],
            ['Banter', 'League of Legends'],
            ['Banter', 'Strategy games'],
            ['Banter', 'Game discussions'],
            ['Banter', 'Personal'],
            ['Banter', 'Finance'],
            ['Banter', 'Movies & Series'],
            ['Banter', 'Games'],
            ['Banter', 'Achievements & Bragging'],
            ['Banter', 'News around the World'],
            ['Banter', 'Internet & Tech'],
            ['Banter', 'Reallife'],
            ['Banter', 'Graphics'],
            ['Banter', 'Graphic Resources'],
            ['Banter', 'Paid Graphic Work'],
            ['Banter', 'International Lounge'],
            ['Banter', 'Español'],
            ['Banter', 'Deutsch'],
            ['Banter', 'Français'],
            ['Banter', 'Dutch'],
            ['Cracking', 'Guides, Tuts & Assistance'],
            ['Cracking', 'Tools'],
            ['Cracking', 'Configs'],
            ['Cracking', 'Cracking Tools'],
            ['Cracking', 'Cracking Tutorials'],
            ['Cracking', 'Cracking Help'],
            ['Cracking', 'OpenBullet'],
            ['Cracking', 'Sentry MBA'],
            ['Cracking', 'BlackBullet'],
            ['Cracking', 'STORM'],
            ['Cracking', 'SNIPR'],
            ['Cracking', 'Streaming'],
            ['Cracking', 'Gaming'],
            ['Cracking', 'Porn'],
            ['Cracking', 'Proxies'],
            ['Leaks', 'Tutorials, Guides, etc.'],
            ['Leaks', 'Webmaster Resources'],
            ['Leaks', 'Cracked Programs'],
            ['Leaks', 'Shopping Deals & Discounts'],
            ['Leaks', 'Source codes'],
            ['Leaks', 'Other leaks'],
            ['Leaks', 'Requests'],
            ['Leaks', 'Leaked E-Books'],
            ['Leaks', 'Youtube, Twitter, and FB bots'],
            ['Money'],
            ['Coding'],
            ['Marketplace'],
            ['Premium Section']
        ]

        self.filter_categories(category_dict, categories_to_remove)
        return category_dict

    def convert_str_to_date(self, time_str): #cracked.to 같은 경우엔 추상적으로 업로드 시간을 나타내기에 제공된 이외의 구체적인 항목은 0으로 기입
        current_time = datetime.datetime.now()
        if re.search(r'year', time_str):
            updated_time = re.findall(r'(\d+) year', time_str)[0]
            years = int(updated_time[0])
            return current_time.replace(year=current_time.year-years,month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        elif re.search(r'month', time_str):
            updated_time = re.findall(r'(\d+) month', time_str)[0]
            months = int(updated_time[0])
            return current_time.replace(month=current_time.month-months,day=1, hour=0, minute=0, second=0, microsecond=0)
        elif re.search(r'day', time_str):
            updated_time = re.findall(r'(\d+) day', time_str)[0]
            days = int(updated_time[0])
            return current_time.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=days)
        elif re.search(r'hour', time_str):
            updated_time = re.findall(r'(\d+) hour', time_str)[0]
            hour = int(updated_time[0])
            return current_time.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=hour)
        elif re.search(r'minute', time_str):
            updated_time = re.findall(r'(\d+) minute', time_str)[0]
            minutes = int(updated_time[0])
            return current_time.replace(second=0, microsecond=0) - datetime.timedelta(minutes=minutes)
        elif re.search(r'week', time_str):
            updated_time = re.findall(r'(\d+) week', time_str)[0]
            weeks = int(updated_time[0])
            return current_time.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=weeks*7)
        elif re.search(r'second', time_str):
            updated_time = re.search(r'(\d+) second', time_str)[0]
            seconds = int(updated_time[0])
            return current_time.replace(microsecond=0) - datetime.timedelta(seconds=seconds)

    def parse(self, response): # The Callback Function that parse the HTTP Resp
                               # Register the Callback Function to [crawl each posts]
        meta = response.meta.copy()

        page_idx = meta['page']
        next_page_idx = page_idx + 1
        bs_obj = BeautifulSoup(response.text, 'lxml')
        thread_container = bs_obj.select('tr.inline_row.forum2')
        
        logger.debug('thread count: ' + str(len(thread_container)))

        for thread in thread_container:
            meta['replies'] = thread.select('td')[2].select_one('span#stats-count').text.strip().replace('.', '')
            meta['views'] = thread.select('td')[3].select_one('span#stats-count').text.strip().replace('.', '')
            meta['response_url'] = urlparse.urljoin(self.base_url, thread.select_one('a')['href'])
            link = thread.select_one('span.subject_new') if thread.select_one('span.subject_new') else thread.select_one('span.subject_old')
            meta['thread_id'] = re.findall(r'tid_(\d*)', link['id'])[0]
            author_container = thread.select_one('div.author.smalltext')
            author_container.select_one('span.thread-date').decompose()
            meta['author'] = author_container.text.strip()
            meta['updated_at'] = self.convert_str_to_date(thread.select_one('span.lastpost.smalltext.thread-date').text.strip())

            yield Request(url=meta['response_url'], 
                        callback=self.parse_thread, headers=self.request_header,
                        meta=meta)
        if not bs_obj.select_one('a.pagination_next'):
            return

        next_page_url = response.url.split('?')[0] + '?page=' + str(next_page_idx)
        meta['response_url'] = next_page_url
        meta['page'] = next_page_idx

        yield Request(url=meta['response_url'],
                    callback=self.parse, headers=self.request_header,
                    meta=meta)

    def parse_thread(self, response):

        meta = response.meta.copy()
        bs_obj = BeautifulSoup(response.text, 'lxml')
        
        #get from meta
        thread_id = meta.get('thread_id')
        replies = meta.get('replies')
        views = meta.get('views').replace('-', '0')
        thread_url = meta.get('response_url')
        category = meta.get('category')
        updated_at = meta.get('updated_at')
        author = meta.get('author')
        
        #get from parsing
        thread_upload_time_str = bs_obj.select_one('span.post_date').text.strip()
        upload_at = self.convert_str_to_date(thread_upload_time_str)
        title = bs_obj.select_one('div.thread-header > h1').text.strip()
        contents_container = bs_obj.select_one('div.post_body.scaleimages')
        hidden_contents_exist = True if bool(contents_container.select_one('div.hidden-content')) else False
        if hidden_contents_exist:
            contents_container.select_one('div.hidden-content').decompose()
        contents = contents_container.text.strip()

        item = dict(
            thread_id=thread_id,
            title=title,
            upload_at=upload_at,
            updated_at=updated_at,
            category=category,
            author=author,
            views=views,
            replies=replies,
            contents=contents,
            hidden_contents_exist=hidden_contents_exist,
            thread_url=thread_url,
            spider=self.name,
            created_at=datetime.datetime.now(),
            domain=self.allowed_domains[0]
        )
        
        yield Thread(item)
