# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import requests

import scrapy
from fourgol.items import Thread

import boto3
from botocore.exceptions import ClientError
import datetime

class FourgolMongoPipeline(object):

    collection_name = 'Thread'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db[self.collection_name].create_index([("spider", pymongo.ASCENDING), (
            "thread_id", pymongo.ASCENDING)], unique=True)
    
    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item_dict = dict(item)
        query_filter = self.__generate_query_from_dict(item_dict)
        
        self.db[self.collection_name].update(
            query_filter,
            self.__generate_thread_dict(item_dict),
            upsert=True
        )
        return item
    
    def __generate_query_from_dict(self, item_dict):
        query_filter = {"spider": item_dict['spider']}
        if 'thread_id' in item_dict:
            query_filter.update({"thread_id" : item_dict['thread_id']})
        
        return query_filter

    def __generate_thread_dict(self, item_dict):
        try:
            result = next(self.db[self.collection_name].find(
                self.__generate_query_from_dict(item_dict)
            ))
        except StopIteration:
            result = None
        
        if result:
            item_dict['created_at'] = result.get('created_at', item_dict['created_at'])
        
        return item_dict
    
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        if crawler.settings.get('RUN_PROFILE') in 'dev':
            return cls(
                mongo_uri = crawler.settings.get('LOCAL_MONGO_URI'),
                mongo_db = crawler.settings.get('LOCAL_MONGO_DATABASE', 'fourgol')
            )
        elif crawler.settings.get('RUN_PROFILE') in 'YH':
            return cls(
                mongo_uri=crawler.settings.get('MONGODB_URI'),
                mongo_db=crawler.settings.get('MONGODB_DATABASE', 'fourgol')
            )

class FourgolDynamoPipeline(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name,
                    table_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.table_name = table_name
        self.start_date = str(datetime.datetime.today().strftime('%Y-%m-%d'))
    
    @classmethod
    def from_crawler(cls, crawler):
        aws_access_key_id = crawler.settings.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = crawler.settings.get('AWS_SECRET_ACCESS_KEY')
        region_name = crawler.settings.get('AWS_REGION_NAME')
        table_name = crawler.settings.get('AWS_DYNAMODB_TABLE_NAME')
        return cls(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name = region_name,
            table_name = table_name
        )
    
    def open_spider(self, spider):
        db = boto3.resource(
            'dynamodb',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        try:
            response = db.create_table(
                TableName = self.table_name,
                KeySchema = [
                    {
                        'AttributeName': 'HASH',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions = [
                    {
                        'AttributeName': 'HASH',
                        'AttributeType': 'S',
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        except ClientError:
            pass

        self.table = db.Table(self.table_name)
    
    def close_spider(self, spider):
        r = requests.get("https://inv8es9ma5.execute-api.us-east-1.amazonaws.com/fourgol/lambda-pipeline/"
            + self.table_name.split("_")[0] + "?cDate=" + self.start_date)
        self.table = None
    
    def process_item(self, item, spider):
        item_dict=self.__generate_thread_dict(dict(item))

        self.table.put_item(
            TableName=self.table_name,
            Item=item_dict
        )
        return item
    
    def __generate_thread_dict(self, item_dict):
        item_dict['HASH'] = item_dict['thread_id'] + item_dict['views'] + item_dict['replies']

        try:
            result = self.table.get_item(
                Key={"HASH":item_dict['HASH']}
            )
        except ClientError:
            result = None

        if result:  #기존 항목과 변화가 없을 경우 생성시간 보존
            item_dict['created_at'] = result.get('created_at', item_dict['created_at'])

        item_dict['updated_at'] = self.time_formatter(item_dict['updated_at'])
        item_dict['upload_at'] = self.time_formatter(item_dict['upload_at'])
        item_dict['created_at'] = self.time_formatter(item_dict['created_at'])

        if not item_dict['contents']:
            item_dict['contents'] = '*'

        return item_dict

    def time_formatter(self, value):
        if isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        elif isinstance(value, datetime.time):
            return value.strftime('%H:%M:%S')
        else:
            return value