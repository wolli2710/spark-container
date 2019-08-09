from pyspark.sql import SparkSession
import argparse
import pyspark.sql.functions as func
import time
from bs4 import BeautifulSoup
import requests
import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class TestUseCase():
    def __init__(self, environment, source_companies, destination):
        ms = int(round(time.time() * 1000))
        master = "yarn"
        if environment == "test":
            master = "local[*]"
        self.spark = (SparkSession.builder
          .master(master)
          .appName('example_use_case_'+str(ms) )
          .enableHiveSupport()
          .getOrCreate())

    def read_parquet(self, path):
        return self.spark.read.parquet(path)

    def read_csv(self, file):
        return self.spark.read.csv(file, header=True, sep=";")

    def write_parquet(self, dataset, dataframe):
        dataframe.write.parquet(dataset, mode="overwrite")

    def stop_spark_session(self):
        self.spark.stop()

    def get_web_data(self, url):
        page = requests.get(url)
        return page.content


    def get_web_data_with_element(self, url, element):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox') # required when running as root user. otherwise you would get no sandbox errors.
        driver = webdriver.Chrome(executable_path='/home/dev/chromedriver', chrome_options=chrome_options,
          service_args=['--verbose', '--log-path=/tmp/chromedriver.log'])

        driver.get(url)
        #Wait till Javascript is loaded
        delay = 3 # seconds
        # while True:
        #     try:
        #         print(driver.find_elements_by_xpath("//th[contains(text(), 'Kürzel')]"))
        #         WebDriverWait(driver, delay).until(EC.presence_of_element_located( driver.find_elements_by_xpath("//th[contains(text(), '"+element+"')]") ) )
        #         print("Page is ready!")
        #         break # it will break from the loop once the specific element will be present.
        #     except:
        #         print("Loading took too much time!-Try again")
        # page = driver.page_source
        return page

    def parse_company_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        print(soup.find(text='Kürzel'))

        short_name = soup.find(text='Kürzel').next_element.getText()
        print(self.company_data)
        self.company_data.append( {short_name:{}} )

    def parse_company_list(self, content, params):
        tds = []
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.find(class_='panel-primary')
        table_rows = table.find_all('tr')

        for row in table_rows:
            result = row.find_all('td')
            if len(result) > 1:
                tds.append(result[0].getText())
        return tds

    def get_data(self):
        data = self.get_web_data("https://www.wienerborse.at/emittenten/aktien/unternehmensliste/")
        return self.parse_company_list(data, "")

    def start_calculation(self):
        self.company_data = []
        companies = self.get_data()
        for company in companies:
            company_website = self.get_web_data_with_element("https://www.wienerborse.at/marktdaten/aktien-sonstige/preisdaten/?ISIN=" + company, "Kürzel")
            self.parse_company_page( company_website )
        return self.company_data

if __name__ == "__main__":
    # define available arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-environment", required=True)
    parser.add_argument("-source_companies", required=True)
    parser.add_argument("-destination", required=True)
    # parse arguments and return them
    args = parser.parse_args()

    source_companies = args.source_companies
    destination = args.destination
    environment = args.environment

    test_example_use_case = TestUseCase(environment, source_companies, destination)
    test_example_use_case.stop_spark_session()
