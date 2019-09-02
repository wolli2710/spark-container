from __future__ import division

from pyspark.sql import SparkSession
import argparse
from datetime import date
import pyspark.sql.functions as func
import matplotlib.pyplot as plt
import os
import numpy as np
import json
import time
import requests
from bs4 import BeautifulSoup
from pyspark.sql.types import *
import importlib

class ExampleUseCase():
    float_formatter = lambda self, x: "%.2f" % x

    def __init__(self, spark_session, environment):
        ms = int(round(time.time() * 1000))
        master = "yarn"
        if environment == "test":
            master = "local[*]"
        self.spark = (SparkSession.builder
          .master(master)
          .appName('example_use_case_'+str(ms) )
          .enableHiveSupport()
          .getOrCreate())
        self.helpers = {}
        self.data = {}
        self.load_helpers()

    def load_helpers(self):
        with open('use_case/helpers/list.json') as json_file:
            data = json.load(json_file)
            for obj in data:
                self.data[obj['name']] = obj
                mod = importlib.import_module("use_case.helpers." + obj['path'])
                met = getattr(mod, obj['prefix'])
                self.helpers[obj['name']] = met

    def run(self):
        for helper in self.helpers:
            self.helpers[helper]().process(self.spark)

    def file_path(self, file_name, dir=""):
        base_path = os.path.dirname(os.path.realpath(__file__)) + "/tests/fixtures/"
        return os.path.dirname(base_path) + "/" + dir + file_name


    # def parse_yahoo_request(self, html_doc):
    #     data = []
    #     soup = BeautifulSoup(html_doc, 'html.parser')
    #     table = soup.find('table', attrs={'data-test':'historical-prices'})
    #     tbody = table.find('tbody')
    #     rows = tbody.find_all('tr')
    #     for row in rows:
    #         cols = row.find_all('td')
    #         cols = [ele.text for ele in cols]
    #         self.prepare_rows(cols, data)
    #     return data
    #
    # def prepare_rows(self, row, data):
    #     if len(row) >= 4:
    #         data.insert(0, [ele.replace(",", "").replace("-", "0") for ele in row[1:-1] if ele])
    #



    # def get_stock_data_from_file_source(self):
    #     data = []
    #     file_name = self.shortage + ".csv"
    #     file_p = self.file_path(file_name, "input/")
    #     result = self.read_file(file_p, "csv")
    #     if result:
    #         current_list = result.rdd.map(lambda row : list(row) ).collect()
    #         for row in current_list:
    #             self.prepare_rows(row, data)
    #         self.data = data
    #         self.prepare_data()

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
