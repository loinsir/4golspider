# -*- coding: utf-8 -*-

import logging
import datetime
from time import strptime
import json
import urllib.parse as urlparse
import re

from scrapy import Request
from bs4 import BeautifulSoup
import requests

from fourgol.fourgol_spider import FourgolSpider
from fourgol.settings import AWS_DYNAMODB_TABLE_NAME
from fourgol.items import Thread

logger = logging.getLogger('raidforums')


class RaidforumsSpider(FourgolSpider):
    name = 'raidforums' # scrapy crawl raidforums

    allowed_domains = ['raidforums.com']

    #raidforum 특성상 너무 많은 요청을 보내면 요청 차단당하므로...
    custom_settings = {
        'DOWNLOAD_DELAY': '2',
        'CONCURRENT_REQUESTS_PER_DOMAIN' : '8',
        'CONCURRENT_REQUESTS_PER_IP':'8',
        'AWS_DYNAMODB_TABLE_NAME' : AWS_DYNAMODB_TABLE_NAME % name
    }

    def __init__(self, name=None, **kwargs):
        self.base_url = 'https://raidforums.com/'
        self.default_page = 1
        self.request_header = {
	        "Accept": "*/*",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4'
        }
        self.categories_to_remove = [
            ['General', 'Announcements'],
            ['General', 'Introductions'],
            ['General', 'World News'],
            ['General', 'The Lounge'],
            ['General', 'Anime & Manga'],
            ['General', 'Not Safe For Work'],
            ['Raiding'],
            ['Raiding Teams'],
            ['Official Teams'],
            ['Tutorials'],
            ['PC'],
            ['Phones'],
            ['Anything Goes'],
            ['Staff'],
            ['Online Accounts', 'Porn'],
            ['Services'],
            ['Programs']
        ]

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

    def __generate_category_dict(self): # crawl the categories using bs4
        category_dict = dict()
        r = requests.get(self.base_url, headers = self.request_header)
        bs_obj = BeautifulSoup(r.text, 'html.parser')

        section_list = bs_obj.select('section#forums > div')
        for section in section_list:
            section_name = section.select_one('thead strong').text.strip()
            category_dict[section_name] = dict()
            board_list = section.select('tbody > tr')

            for board in board_list:
                board_info = board.select('td')[1].select_one('a')
                board_name = board_info.text.strip()
                board_url = board_info['href']
                category_dict[section_name][board_name] = board_url
        
        self.filter_categories(category_dict, self.categories_to_remove)
        
        return category_dict

    def convert_str_to_date(self, time_str):
        current_time = datetime.datetime.now()
        if re.search(r'Yesterday', time_str):
            updated_time = re.findall(r'Yesterday at (\d{2}):(\d{2}) (\w+)', time_str)[0]
            hour = int(updated_time[0]) + 12 if updated_time[2] == 'PM' and int(updated_time[0]) < 12 else int(updated_time[0])
            minute = int(updated_time[1])
            return current_time.replace(hour=hour, minute=minute) - datetime.timedelta(days=1)
        elif re.search(r'hour', time_str):
            updated_time = re.findall(r'(\d+) hour', time_str)[0]
            hour = int(updated_time)
            return current_time - datetime.timedelta(hours=hour)
        elif re.search(r'minute', time_str):
            updated_time = re.findall(r'(\d+) minute', time_str)[0]
            minutes = int(updated_time)
            return current_time - datetime.timedelta(minutes=minutes)
        else:
            updated_time = re.findall(r'(\w+) (\d{2}), (\d{4}) at (\d{2}):(\d{2}) (\w+)', time_str)[0]
            month_abbr = updated_time[0][0:3]
            month = strptime(month_abbr,'%b').tm_mon
            date = int(updated_time[1])
            year = int(updated_time[2])
            hour = int(updated_time[3])
            minute = int(updated_time[4])
            if updated_time[5] == 'PM' and hour < 12:
                hour += 12
            return datetime.datetime(year=year, month=month, day=date, hour=hour, minute=minute)

    def parse(self, response): # The Callback Function that parse the HTTP Resp
                               # Register the Callback Function to [crawl each posts]
        meta = response.meta.copy()
        
        page_idx = meta['page']
        next_page_idx = page_idx + 1
        bs_obj = BeautifulSoup(response.text, 'lxml')
        thread_container = bs_obj.select('tr.forum-display__thread.inline_row')
        
        logger.debug('thread count: ' + str(len(thread_container)))

        for thread in thread_container:
            meta['replies'] = thread.select('td')[2].text.strip().replace(',', '')
            meta['views'] = thread.select('td')[3].text.strip().replace(',', '')
            meta['response_url'] = urlparse.urljoin(self.base_url, thread.select_one('a.forum-display__thread-name')['href'])
            meta['thread_id'] = re.findall(r'tid_(\d*)', thread.select_one('span.subject_new')['id'])[0]
            meta['author'] = thread.select_one('span.author.smalltext').text.strip().replace('by ', '')
            meta['updated_at'] = self.convert_str_to_date(thread.select_one('span.lastpost.smalltext').text.strip())
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
        views = meta.get('views')
        thread_url = meta.get('response_url')
        category = meta.get('category')
        updated_at = meta.get('updated_at')
        upload_at = meta.get('upload_at')
        author = meta.get('author')
        
        #get from parsing
        thread_upload_time_str = bs_obj.select_one('div.thread-info__datetime').text.strip()
        upload_at = self.convert_str_to_date(thread_upload_time_str)
        title = bs_obj.select_one('span.thread-info__name.rounded').text.strip()
        contents_container = bs_obj.select_one('div.post_content > div.post_body.scaleimages')
        hidden_contents_exist = True if bool(contents_container.select_one('div.hidden-content.rounded')) else False
        if hidden_contents_exist:
            contents_container.select_one('div.hidden-content.rounded').decompose()
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