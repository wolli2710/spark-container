import logging
import unittest
from pyspark.sql import SparkSession

class PySparkTest(unittest.TestCase):

  @classmethod
  def suppress_py4j_logging(cls):
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.ERROR)

  @classmethod
  def create_testing_pysparkSession(cls):
    return (SparkSession.builder
      .master('local')
      .appName('my-local-testing-pyspark-context')
      .enableHiveSupport()
      .getOrCreate())

  @classmethod
  def setUpClass(cls):
    cls.suppress_py4j_logging()
    cls.spark = cls.create_testing_pysparkSession()

  @classmethod
  def tearDownClass(cls):
    cls.spark.stop()
