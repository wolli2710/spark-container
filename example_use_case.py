from __future__ import division

import argparse
from datetime import date
import pyspark.sql.functions as func
import matplotlib.pyplot as plt
import os
import numpy as np
import json
import requests
from bs4 import BeautifulSoup
from pyspark.sql.types import *
import importlib

class ExampleUseCase():
    float_formatter = lambda self, x: "%.2f" % x

    def __init__(self, spark_session, shortage, obj):
        self.spark_session = spark_session
        self.shortage = shortage
        self.obj = obj
        self.result_file_path = self.file_path(shortage, "results/") + ".png"
        self.helpers = {}
        self.load_helpers()
        for key,method in self.helpers.items():
            method()

    def load_helpers(self):
        with open('helpers/list.json') as json_file:
            data = json.load(json_file)
            for obj in data:
                mod = importlib.import_module(obj['path'])
                met = getattr(mod, obj['name'])
                self.helpers[obj['name']] = met

    def file_path(self, file_name, dir=""):
        base_path = os.path.dirname(os.path.realpath(__file__)) + "/tests/fixtures/"
        return os.path.dirname(base_path) + "/" + dir + file_name

    def draw_result(self):
        cSchema = StructType([StructField("Date", StringType())\
                      ,StructField("Open", StringType())\
                      ,StructField("High", StringType())\
                      ,StructField("Low", StringType())\
                      ,StructField("Close", StringType())\
                      ,StructField("Adj Close", StringType())\
                      ,StructField("Volume", StringType())])
        dc = self.spark_session.createDataFrame(self.data, schema=cSchema)
        y = [float(i.Close.replace(',','')) for i in dc.select("Close").collect()]
        x = [str(i.Date ) for i in dc.select("Date").collect()]
        plt.figure(figsize=(50,10))
        plt.plot(x, y, '-b', label='loss')
        plt.title(self.shortage)
        plt.savefig(self.file_path)
        plt.clf()

    def read_file(self, input_file, file_format='parquet', sep=',', encoding='utf-8'):
        if not (os.path.isfile(input_file)):
            return
        if file_format == 'parquet':
            return self.spark_session.read.parquet(input_file)
        elif file_format == 'csv':
            return self.spark_session.read.csv(input_file, header=True, sep=sep, encoding=encoding)

    def write_file(self, spark_session, input_file, inputData, file_format, mode=None):
        if file_format == 'parquet':
            inputData.write.parquet(input_file+".csv", mode=mode)
        elif file_format == 'csv':
            inputData.write.csv(input_file+".csv", mode=mode, sep=';', header=True)

    def get_stock_data_from_web_source(self):
        response = requests.get("https://finance.yahoo.com/quote/"+self.shortage+"/history?p="+self.shortage)
        self.data = self.parse_yahoo_request(response.text)
        self.prepare_data()

    def parse_yahoo_request(self, html_doc):
        data = []
        soup = BeautifulSoup(html_doc, 'html.parser')
        table = soup.find('table', attrs={'data-test':'historical-prices'})
        tbody = table.find('tbody')
        rows = tbody.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text for ele in cols]
            self.prepare_rows(cols, data)
        return data

    def prepare_rows(self, row, data):
        if len(row) >= 4:
            data.insert(0, [ele.replace(",", "").replace("-", "0") for ele in row[1:-1] if ele])

    def get_stock_data_from_file_source(self):
        data = []
        file_name = self.shortage + ".csv"
        file_p = self.file_path(file_name, "input/")
        result = self.read_file(file_p, "csv")
        if result:
            current_list = result.rdd.map(lambda row : list(row) ).collect()
            for row in current_list:
                self.prepare_rows(row, data)
            self.data = data
            self.prepare_data()

    def prepare_data(self):
        if( hasattr(self, "data") ):
            n = np.array(self.data)
            np.set_printoptions(formatter={'float_kind':self.float_formatter})
            n = n.astype(np.float)
            self.average = np.average(n, axis=0)
            self.median = np.median(n, axis=0)
            self.max = np.max(n, axis=0)
            self.std = np.std(n, axis=0)
            self.last = n[-1]

    def predict_low_cost_high_value(self):
        if( hasattr(self, "data") ):
            threshold = 80.0
            # if(  ( any( x < threshold for x in (100/self.max) * self.last) )  ):
            print(self.obj["name"])
            # print(self.last - self.median)
            # print(self.last - self.average)
            # print(self.median)
            # print(self.average)
            # print(self.last)
            # print(self.std)
            print(self.max)
            print(self.last)
            print( ( (100/self.average) * self.last ) )
            print( ( (100/self.max) * self.last ) )

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('-source_fahrzeug', required=True)
#     parser.add_argument('-destination', required=True)
#     parser.add_argument('-format', choices=['csv', 'parquet'], default='csv', type=str.lower)
#     parser.add_argument('-local', action='store_true')
#     args = parser.parse_args()
#
#     fzgPath = args.source_fahrzeug
#     dest_path = args.destination
#     file_format = args.format
#
#     # define master
#     master = 'yarn'
#     if args.local:
#         master = 'local'
#
#     spark_session = spark_session.builder.master(master).appName('example_use_case').getOrCreate()
#     ExampleUseCase(spark_session=spark_session, fzgPath=fzgPath, dest_path=dest_path, file_format=file_format)
#     spark_session.stop()
