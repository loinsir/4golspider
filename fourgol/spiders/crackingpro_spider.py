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

logger = logging.getLogger('crackingpro')


class CrackingproSpider(FourgolSpider):
    name = 'crackingpro' # scrapy crawl raidforums

    allowed_domains = ['www.crackingpro.com']

    custom_settings = {
        'RANDOMIZE_DOWNLOAD_DELAY':'True',
        'AWS_DYNAMODB_TABLE_NAME' : AWS_DYNAMODB_TABLE_NAME % name
    }

    # crawlera_enabled = True
    # crawlera_apikey = ''

    def __init__(self, name=None, **kwargs):
        self.base_url = 'https://www.crackingpro.com'
        self.default_page = 1
        self.request_header = {
	        "Accept": "*/*",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4'
        }
        self.cookies = {}

        logger.debug("%s spider was initialized." % self.name)
        pass

    def start_requests(self): # Register the Callback Function on each categories to [crawl whole Board]
        category_dict = self.__generate_category_dict()
        logger.info('__generate_category_dict %s' % json.dumps(category_dict, ensure_ascii=False))

        meta = dict()
        meta['page'] = self.default_page
        meta['category'] = list()
        meta['depth'] = 1
        meta['cookies'] = self.cookies

        return [Request(url=x['response_url'] + 'page/1/', callback=self.parse, headers=self.request_header, meta=x, cookies=meta['cookies'])
                for x in self.get_categories(category_dict, meta=meta)] # Return the List of scrapy.Request

    def __generate_category_dict(self): # crawl the categories using bs4
        category_dict = dict()
        r = requests.get(self.base_url, headers = self.request_header)
        self.cookies = r.cookies.get_dict()
        bs_obj = BeautifulSoup(r.text, 'html.parser')

        section_list = bs_obj.select('ol.ipsList_reset.cForumList.lkForumList > li')
        for section in section_list:
            section_name = section.select_one('a.lkForum_categoryTitle').text.strip()
            category_dict[section_name] = dict()
            board_list = section.select('ol.ipsDataList.ipsDataList_zebra > li')

            for board in board_list:
                board_info = board.select_one('div.lkForumRow_main').select_one('a')
                board_name = board_info.text.strip()
                board_url = board_info['href']
                category_dict[section_name][board_name] = board_url
        
        categories_to_remove = [
            ['CrackingPro Information'],
            ['CrackingPro LOBBY'],
            ['Free Premium Zone'],
            ['VIP ZONE'],
            ['Active Zone'],
            ['Marketplace'],
            ['EXPLOITING ZONE'],
            ['Website Lounge'],
            ['Graphic Zone'],
            ['Graveyard'],
            ['Cracking Zone', 'Cracking Tools'],
            ['Cracking Zone', 'Cracking Configs'],
            ['Cracking Zone', 'Cracking Tutorials & Information'],
            ['Cracking Zone', 'Proxy Lists & Wordlist'],
            ['Cracking Zone', 'Crackers Support & Requests']
        ]

        self.filter_categories(category_dict, categories_to_remove)
        return category_dict

    def convert_str_to_date(self, time_str): #초 단위 이하는 0처리함
        time_info = re.findall(r'(\d+)\/(\d+)\/(\d+) (\d+)\:(\d+)\s+(\w+)', time_str)[0]
        month = int(time_info[0])
        date = int(time_info[1])
        year = int(time_info[2])
        is_AM = time_info[-1]
        hour = int(time_info[3])
        if is_AM == 'PM' and hour < 12:
            hour += 12
        minute = int(time_info[4])
        return datetime.datetime(year=year, month=month, day=date, hour=hour, minute=minute)

    def parse(self, response): # The Callback Function that parse the HTTP Resp
                               # Register the Callback Function to [crawl each posts]
        meta = response.meta.copy()

        page_idx = meta['page']
        next_page_idx = page_idx + 1
        bs_obj = BeautifulSoup(response.text, 'lxml')
        thread_container = bs_obj.select('li.ipsDataItem.ipsDataItem_responsivePhoto')
        
        logger.debug('thread count: ' + str(len(thread_container)))

        for thread in thread_container:
            meta['replies'] = thread.select('ul.ipsDataItem_stats > li')[0].select_one('span').text.strip()
            meta['views'] = thread.select('ul.ipsDataItem_stats > li')[1].select_one('span').text.strip().replace(',', '')
            meta['response_url'] = thread.select_one('span.ipsType_break.ipsContained > a')['href']
            meta['title'] = thread.select_one('span.ipsType_break.ipsContained > a').text.strip()
            meta['thread_id'] = re.findall(r'topic/(\d+)-', meta['response_url'])[0]
            meta['author'] = thread.select_one('div.ipsDataItem_meta.ipsType_reset.ipsType_light.ipsType_blendLinks > span > a').text.strip().replace('by ', '').replace('+ ', '')
            meta['updated_at'] = self.convert_str_to_date(thread.select_one('li.ipsType_light time')['title'])
            meta['upload_at'] = self.convert_str_to_date(thread.select_one('div.ipsDataItem_main time')['title'])
            yield Request(url=meta['response_url'], 
                        callback=self.parse_thread, headers=self.request_header,
                        meta=meta)

        if 'ipsPagination_inactive' in bs_obj.select_one('li.ipsPagination_next')['class']:
            return

        next_page_url = response.url.split('page')[0] + 'page/' + str(next_page_idx) + '/'
        meta['response_url'] = next_page_url
        meta['page'] = next_page_idx

        yield Request(url=meta['response_url'],
                    callback=self.parse, headers=self.request_header,
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
        upload_at = meta.get('upload_at')
        
        #get from parsing
        contents_container = bs_obj.select_one('div.ipsType_normal.ipsType_richText.ipsContained')
        hidden_contents_exist = True if contents_container.select_one('div.ipsMessage.ipsMessage_error strong') else False
        if hidden_contents_exist:
            contents_container.select_one('div.ipsMessage.ipsMessage_error strong').decompose()
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
