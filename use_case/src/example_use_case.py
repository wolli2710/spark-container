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

    def __init__(self, spark_session, environment, source, destination):
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
        self.source = source
        self.destination = destination

    def load_helpers(self):
        with open('use_case/helpers/list.json') as json_file:
            data = json.load(json_file)
            for obj in data:
                self.data[obj['name']] = obj
                mod = importlib.import_module("use_case.helpers." + obj['path'])
                met = getattr(mod, obj['prefix'])
                self.helpers[obj['name']] = met

    def run(self):
        # for helper in self.helpers:
        #     self.helpers[helper]().ingest(self.spark)
        # return
        for helper in self.helpers:
            self.helpers[helper]().process(self.spark, self.source)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-source', required=True)
    parser.add_argument('-destination', required=True)
    parser.add_argument('-format', choices=['csv', 'parquet'], default='csv', type=str.lower)
    parser.add_argument('-environment', default='prod')
    args = parser.parse_args()

    source = args.source
    destination = args.destination
    format = args.format

    # define master
    master = 'yarn'
    if args.environment != 'prod':
        master = 'local'

    spark_session = spark_session.builder.master(master).appName('example_use_case').getOrCreate()
    use_case = ExampleUseCase(spark_session=spark_session, environment="prod", source=source, destination=destination)
    use_case.run()
    spark_session.stop()
