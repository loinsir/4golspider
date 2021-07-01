FROM python:3.6

MAINTAINER loinsir <a9327370@gmail.com>

ENV SCRAPY_SETTINGS_MODULE fourgol.settings
RUN mkdir -p /app
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN python setup.py install
EXPOSE 5000
CMD scrapyd & scrapydweb --disable_auth --disable_logparser --scrapyd_server 127.0.0.1:6800