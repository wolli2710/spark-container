import unittest
import sys
import os
import glob
import subprocess
import json

sys.path.append('./use_case/tests')
sys.path.append('./use_case/helpers')
sys.path.append('./use_case/src')

from unittest.mock import patch
from base_spark_test import PySparkTest
from operator import add
from example_use_case import ExampleUseCase
from pyspark.sql.types import *
from googlefinance.get import get_code

def file_path(file_name, dir=""):
    base_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.dirname(base_path) + "/" + dir + "/" + file_name

class SimpleTest(PySparkTest):
    # def test_basic_spark_behaviour(self):
    #     test_rdd = self.spark.sparkContext.parallelize(['cat dog mouse','cat cat dog'], 2)
    #     results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
    #     expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]
    #     self.assertEqual(set(results), set(expected_results))

    # def test_read_file(self):
    #     shortage = "VOW.DE"
    #     obj = ExampleUseCase(spark_session=self.spark, shortage=shortage)
    #     file_format = 'csv'
    #     input_file = file_path("CSV.csv", "tests/fixtures")
    #     input_data = obj.read_file(self.spark, input_file, file_format)
    #     result = input_data.select("*")

    # def test_get_stock_data_from_file(self):
    #     use_case_objects = []
    #     data = {}
    #     file_paths = glob.glob("./tests/fixtures/input/*.csv")
    #     data["shortages"] = [p[p.rfind("/")+1   :p.rfind(".csv")] for p in file_paths]
    #     for shortage in data["shortages"]:
    #         use_case_objects.append(ExampleUseCase(spark_session=self.spark, shortage=shortage))
    #     [data.get_stock_data_from_file_source() for data in use_case_objects]
    #     [data.predict_low_cost_high_value() for data in use_case_objects]


    # @patch('alfred_aftersales_uc3_pickerl.read_file', return_value=self.spark.createDataFrame())
    # @patch('example_use_case.ExampleUseCase.write_file', return_value=None)
    # @patch('example_use_case.ExampleUseCase.draw_result', return_value=None)
    # def test_example_use_case(self, draw_result):
    def test_get_stock_data_from_web(self):
        source = "/data/stocks/"
        destination = ""
        use_case = ExampleUseCase(spark_session=self.spark, environment="test", source=source, destination=destination)
        use_case.run()

        # [data.get_company_list() for data in use_case_objects]


        # [data.get_stock_data_from_web_source() for data in use_case_objects]
        # [data.predict_low_cost_high_value() for data in use_case_objects]

    # def test_google_get(self):
        # print([x for x in get_code('NASDAQ')])

    # def test_draw_result(self):
    #     use_case_objects = []
    #     data = {}
    #     file_paths = glob.glob("./tests/fixtures/input/*.csv")
    #     data["shortages"] = [p[p.rfind("/")+1   :p.rfind(".csv")] for p in file_paths]
    #     for shortage in data["shortages"]:
    #         use_case_objects.append(ExampleUseCase(spark_session=self.spark, shortage=shortage))
    #     [data.get_stock_data_from_file_source() for data in use_case_objects]
    #     [data.predict_low_cost_high_value() for data in use_case_objects]
    #     # [data.draw_result() for data in use_case_objects]
    #
