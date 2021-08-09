# 4gol-spider

KITRI BOB-8th 4gol team Deep Web Scraper

## fourgol_spider.py
all spiders inherit FourgolSpider

## structure
#### start_requests()
`input: None`

parsing base_url

#### parse()
`input: response`

parsing thread list pages

callback function : parse_thread

#### parse_thread()
`input: response`

parsing each thread page

#### __generate_category_dict()
`input: None`

generate category dictionary

#### convert_str_to_date()
`input: time_str`

converting time information string into datetime.datetime format 

## item convention
all empty fields are `None`
```
thread_id = Field()              :string (required)
title = Field()                  :string
upload_at = Field()              :datetime object
updated_at = Field()             :datetime object
category = Field()               :list(of string) [leak, database]
author = Field()                 :string   (uploader)
views = Field()                  :int
contents = Field()               :string
hidden_contents_exist = Field()  :bool
thread_url = Field()             :string
spider = Field()                 :string
created_at = Field()             :datetime object
domain = Field()                 :string 
```
thread_id field use for mongoDB Deduplication(pipelines.py)
## Usage(debug)

check settings.py

RUNPROFILE = 'dev'

```
cd fourgol
scrapy crawl <spider name>
```
check your local mongoDB:fourgol collection:Thread 

~~## About Scrapyd~~

~~#### Web Interface~~
~~http://3.89.221.105:6800/~~~~

~~#### Schedule a spider run~~(deleted)
```
curl http://3.89.221.105:6800/schedule.json -d project=fourgol -d spider=<spider_name>
```

~~#### Cancel a spider run~~(deleted)
```
curl http://3.89.221.105:6800/cancel.json -d project=fourgol -d job=<job_ID>
```

# sources
[scrapy](https://scrapy.org/)

[scrapyd](https://scrapyd.readthedocs.io/en/stable/#)
