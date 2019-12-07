import requests
from bs4 import BeautifulSoup
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import time
import calendar
import pandas as pd
import hjson

class BaseIngest():
    float_formatter = lambda self, x: "%.2f" % x

    def ingest(self, spark):
        self.spark = spark
        company_list = self.get_company_list()
        for company in company_list:
            self.prepare_company(company)

    def get_web_data(self, url):
        page = requests.get(url)
        return page.content

    def get_stock_data_from_web_source(self, shortening_full):
        current_timestamp = str( calendar.timegm(time.gmtime()) )
        url = "https://query1.finance.yahoo.com/v8/finance/chart/"+shortening_full+"?symbol="+shortening_full+"&interval=1d&period1=1&period2="+current_timestamp+"&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=t5QZMhgytYZ&corsDomain=finance.yahoo.com"
        response = requests.get(url)
        json_doc = hjson.loads(response.text)
        data = self.parse_yahoo_api_request(json_doc)
        dividends = self.handle_dividends(json_doc)
        self.save_raw_data(data, shortening_full)
        self.save_raw_data(dividends, shortening_full+".div")

    def parse_yahoo_api_request(self, doc):
        timestamp = doc['chart']['result'][0]['timestamp']
        data = doc['chart']['result'][0]['indicators']['quote'][0]
        return pd.DataFrame({'timestamp': timestamp, 'high': data["high"], 'low': data["low"], 'open': data["open"], 'close': data["close"], 'volume': data["volume"] })

    def handle_dividends(self, doc):
        div = doc['chart']['result'][0]['events']['dividends']
        timestamps = []
        amounts = []
        for key, value in div.items():
            timestamps.append(key)
            amounts.append(value['amount'])
        return pd.DataFrame({ 'timestamp': timestamps, 'amount': amounts })

    def save_raw_data(self, input_data, shortening_full):
        input_file = "/data/stocks/" + shortening_full
        file_format = "csv"
        self.write_file(input_file, input_data, file_format)
