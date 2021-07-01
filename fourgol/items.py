# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field

class Thread(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    thread_id = Field()              #string
    title = Field()                  #string
    upload_at = Field()              #datetime object
    updated_at = Field()             #datetime object
    category = Field()               #list(of string)   ex) [leak, database]
    author = Field()                 #string    (uploader)
    views = Field()                  #int
    contents = Field()               #string
    hidden_contents_exist = Field()  #bool
    thread_url = Field()             #string
    spider = Field()                 #string
    created_at = Field()             #datetime object
    domain = Field()                 #string 
    replies = Field()                #string