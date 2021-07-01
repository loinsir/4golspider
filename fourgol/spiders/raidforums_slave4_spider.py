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

from fourgol.spiders.raidforums_master_spider import RaidforumsSpider
from fourgol.settings import AWS_DYNAMODB_TABLE_NAME
from fourgol.items import Thread

logger = logging.getLogger('raidforums_slave4')


class RaidforumsSlave4Spider(RaidforumsSpider): # Category: RaidForums>Leaks>Combolists

    name = 'raidforumsslave4'
    custom_settings = {
        'DOWNLOAD_DELAY': '2',
        'CONCURRENT_REQUESTS_PER_DOMAIN' : '8',
        'CONCURRENT_REQUESTS_PER_IP':'8',
        'AWS_DYNAMODB_TABLE_NAME' : AWS_DYNAMODB_TABLE_NAME % 'raidforums'
    }

    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.categories_to_remove = [
            ['General'],
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
            ['Programs'],
            ['Leaks', 'Databases'],
            ['Leaks', 'Games'],
            ['Online Accounts']
        ]