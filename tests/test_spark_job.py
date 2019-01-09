import unittest
import sys
import os
import subprocess

sys.path.append('./tests')

from unittest.mock import patch
from base_spark_test import PySparkTest
from operator import add
from example_use_case import ExampleUseCase
from pyspark.sql.types import *


def file_path(file_name, dir=""):
    base_path = os.path.dirname(os.path.realpath(__file__))
    if not (os.path.exists(base_path+dir)):
        os.makedirs(base_path+dir)
    return os.path.dirname(base_path) + "/" + dir + "/" + file_name

class SimpleTest(PySparkTest):
    def test_basic_spark_behaviour(self):
        test_rdd = self.spark.sparkContext.parallelize(['cat dog mouse','cat cat dog'], 2)
        results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
        expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]
        self.assertEqual(set(results), set(expected_results))

    def test_read_file(self):
        file_format='csv'
        input_file=file_path("CSV.csv", "fixtures")
        dest_path=file_path("file.png", "results")
        inputData =  self.read_file(spark_session, input_file, file_format)
        result = inputData.select("*")
    
    # @patch('alfred_aftersales_uc3_pickerl.read_file', return_value=self.spark.createDataFrame())
    # @patch('example_use_case.ExampleUseCase.write_file', return_value=None)
    # @patch('example_use_case.ExampleUseCase.draw_result', return_value=None)
    # def test_example_use_case(self, draw_result):
    
    def test_example_use_case(self):
        use_case_objects = []
        for shortage in ["VOW.DE", "PAH3.DE", "AAPL", "GOOGL", "O3P.DE", "TSLA", "AMZN"]:
            use_case_objects.append(ExampleUseCase(spark_session=self.spark, shortage=shortage))
        [data.get_stock_data_from_web_source() for data in use_case_objects]
        [data.draw_result() for data in use_case_objects]

    def test_write_to_hdfs(self):
        cSchema = StructType([StructField("Name", StringType())\
                      ,StructField("Value", StringType())])
        data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
        df = self.spark.createDataFrame(data, schema=cSchema)
        path = file_path("example.csv", "tests/results")
        df.write.csv(path)
        df_load = self.spark.read.csv(path)
        df_load.show()
        cmd = 'hdfs dfs -ls tests/results/example.csv'.split() # cmd must be an array of arguments
        files = subprocess.check_output(cmd).strip().split('\n')
        for path in files:
          print (path)
        self.assertEqual(1, 1)
