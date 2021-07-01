# -*- coding: utf-8 -*-

import logging
import datetime
from time import strptime
import json
import urllib.parse as urlparse
import re

from scrapy import Request
from bs4 import BeautifulSoup
from cfscrape import CloudflareScraper
import requests
from selenium import webdriver

from fourgol.fourgol_spider import FourgolSpider
from fourgol.settings import AWS_DYNAMODB_TABLE_NAME
from fourgol.items import Thread

logger = logging.getLogger('nulled')


class NulledSpider(FourgolSpider):
    name = 'nulled' # scrapy crawl raidforums

    allowed_domains = ['www.nulled.to']

    custom_settings = {
        'DOWNLOAD_DELAY': '1',
        'CONCURRENT_REQUESTS_PER_DOMAIN' : '16',
        'CONCURRENT_REQUESTS_PER_IP':'16',
        'RANDOMIZE_DOWNLOAD_DELAY':'True',
        'AWS_DYNAMODB_TABLE_NAME' : AWS_DYNAMODB_TABLE_NAME % name
    }

    def __init__(self, name=None, **kwargs):
        if '_job' in kwargs:
            kwargs.pop('_job')
        super(FourgolSpider, self).__init__(name, **kwargs)
        self.base_url = 'https://www.nulled.to/'
        self.default_page = 1
        self.request_header = {
	        "Accept": "*/*",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4'
        }
        self.scraper = CloudflareScraper()
        logger.debug("%s spider was initialized." % self.name)
        pass

    def start_requests(self): # Register the Callback Function on each categories to [crawl whole Board]
        category_dict = self.__generate_category_dict()
        logger.info('__generate_category_dict %s' % json.dumps(category_dict, ensure_ascii=False))
        meta = dict()
        meta['page'] = self.default_page
        meta['category'] = list()
        meta['depth'] = 1
        meta['cookies'] = self.scraper.cookies.get_dict()

        return [Request(url=x['response_url'] + 'page-2', callback=self.parse, headers=self.request_header, meta=x, cookies=meta['cookies'])
                for x in self.get_categories(category_dict, meta=meta)] # Return the List of scrapy.Request

    def __generate_category_dict(self):
        category_dict = dict()
        r = self.scraper.get(url=self.base_url, headers=self.request_header)
        bs_obj = BeautifulSoup(r.text, 'html.parser')
        section_list = bs_obj.select('div#forumCategories > div')

        temp_forum = str()
        for section in section_list:
            section_title = section.select('h3.maintitle > a')[-1].text.strip()
            category_dict[section_title] = dict()
            forums = section.select('table.ipb_table > tr')[1:]

            for forum in forums:
                if forum.select_one('td.col_c_forum'):
                    forum_title = forum.select_one('h4.forum_name').text.strip()
                    link = forum.select_one('h4.forum_name').select_one('a')['href']
                    category_dict[section_title][forum_title] = link
                    temp_forum = forum_title
                elif forum.select_one('td.ipsList_inline.ipsType_small.subforums'):
                    sub_forums = forum.select_one('td.ipsList_inline.ipsType_small.subforums').select('li > a')
                    category_dict[section_title][temp_forum + '-sub_forum'] = dict()
                    for sub_forum in sub_forums:
                        sub_forum_title = sub_forum.text.strip()
                        link = sub_forum['href']
                        category_dict[section_title][temp_forum + '-sub_forum'][sub_forum_title] = link

        categories_to_remove = [
            ['Nulled'],
            ['Marketplace'],
            ['Coding and Programming'],
            ['Money'],
            ['Hacking'],
            ['VIP Area'],
            ['General & Off Topic Discussion', 'The Lounge'],
            ['General & Off Topic Discussion', 'The Lounge-sub_forum'],
            ['General & Off Topic Discussion', 'Crypto Currencies'],
            ['General & Off Topic Discussion', 'Entertainment'],
            ['General & Off Topic Discussion', 'Entertainment-sub_forum'],
            ['General & Off Topic Discussion', 'Personal Life'],
            ['General & Off Topic Discussion', 'Achievements & Bragging'],
            ['General & Off Topic Discussion', 'Graphics'],
            ['General & Off Topic Discussion', 'Graphics-sub_forum'],
        ]

        self.filter_categories(category_dict, categories_to_remove)
        return category_dict

    def convert_str_to_date(self, time_str): #초 단위 이하는 0처리함
        
        parsed_time_str = re.findall(r'(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)', time_str)[0]
        year = int(parsed_time_str[0])
        month = int(parsed_time_str[1])
        date = int(parsed_time_str[2])
        hours = int(parsed_time_str[3])
        minutes = int(parsed_time_str[4])
        seconds = int(parsed_time_str[5])

        return datetime.datetime(year=year, month=month, day=date, hour=hours, minute=minutes, second=seconds)
    
    def _updated_time_str_to_date(self, time_str):
        current_time = datetime.datetime.now()

        if re.search(r'Today', time_str):
            time_info = re.findall(r'(\d+):(\d+) (\w+)', time_str)[0]
            is_AM = time_info[-1]
            hour = int(time_info[0])
            minute = int(time_info[1])
            if is_AM == 'PM' and hour < 12:
                hour += 12
            return datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day, hour=hour, minute=minute)
        
        elif re.search(r'hour', time_str):  #several hours ago...
            time_info = re.findall(r'(\d+) hour', time_str)[0]
            return datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day, hour=current_time.hour) \
                - datetime.timedelta(hours=int(time_info[0]))
        
        elif re.search(r'minute', time_str):
            time_info = re.findall(r'(\d+) minute', time_str)[0]
            return datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day, hour=current_time.hour) \
                - datetime.timedelta(minutes=int(time_info[0]))
        
        elif re.search(r'Yesterday', time_str):
            time_info = re.findall(r'(\d+):(\d+) (\w+)', time_str)[0]
            is_AM = time_info[-1]
            hour = int(time_info[0])
            minute = int(time_info[1])
            if is_AM == 'PM' and hour < 12:
                hour += 12
            return datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day, hour=current_time.hour, minute=minute) \
                - datetime.timedelta(days=1)
        
        else:
            time_info = re.findall(r'(\d+) (\w+), (\d+)', time_str)[0]
            date = int(time_info[0])
            month = strptime(time_info[1],'%b').tm_mon
            year = int(time_info[-1])

            return datetime.datetime(year=year, month=month, day=date)

    def parse(self, response):
                               
        meta = response.meta.copy()

        page_idx = meta['page']
        next_page_idx = page_idx + 1
        bs_obj = BeautifulSoup(response.text, 'lxml')
        thread_container = bs_obj.select('tr.__topic')[1:]
        
        logger.debug('thread count: ' + str(len(thread_container)))

        for thread in thread_container:
            
            meta['title'] = thread.select_one('td.col_f_content > h4 > a > span').text.strip()
            replies_container = thread.select('td.col_c_stats.hide-mobile.ipsType_small > ul > li')[0]
            replies_container.select_one('span').decompose()
            meta['replies'] = replies_container.text.strip().replace(',', '')
            meta['views'] = thread.select('td.col_c_stats.hide-mobile.ipsType_small > ul > li')[1].text.strip().replace(',', '')
            meta['response_url'] = thread.select_one('td.col_f_content > h4 > a')['href']
            meta['thread_id'] = thread['data-tid']
            meta['author'] = thread.select_one('td.col_f_content > span').text.strip().replace("Started by", '').strip()
            meta['updated_at'] = self._updated_time_str_to_date(thread.select_one('ul.last_post.ipsType_small > li.desc.lighter.blend_links').text.strip())
            # meta['upload_at'] = self.convert_str_to_date(thread.select_one('span.started.smalltext').text.strip())

            yield Request(url=meta['response_url'], 
                        callback=self.parse_thread, headers=self.request_header, cookies=meta['cookies'],
                        meta=meta)

        if not bs_obj.select_one('a[rel="next"]'):
            return

        next_page_url = response.url.split('page')[0] + 'page-' + str(next_page_idx)
        meta['response_url'] = next_page_url
        meta['page'] = next_page_idx

        yield Request(url=meta['response_url'],
                    callback=self.parse, headers=self.request_header, cookies=meta['cookies'],
                    meta=meta)

    def parse_thread(self, response):
        meta = response.meta.copy()
        bs_obj = BeautifulSoup(response.text, 'lxml')
        
        #get from meta
        replies = meta.get('replies')
        views = meta.get('views')
        thread_url = meta.get('response_url')
        title = meta.get('title')
        thread_id = meta.get('thread_id')
        category = meta.get('category')
        author = meta.get('author')
        updated_at = meta.get('updated_at')
        # upload_at = meta.get('upload_at')
        
        #get from parsing
        upload_at = self.convert_str_to_date(bs_obj.select_one('abbr.published')['title'])
        contents_container = bs_obj.select_one('section#nulledPost')
        hidden_contents_exist = True if contents_container.select_one('div.message.error.hiddencontent.hiddencontent-hidden') else False
        if hidden_contents_exist:
            contents_container.select_one('div.message.error.hiddencontent.hiddencontent-hidden').decompose()
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