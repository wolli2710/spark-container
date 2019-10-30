import requests
from bs4 import BeautifulSoup
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.types import *

class BaseIngest():
    float_formatter = lambda self, x: "%.2f" % x

    def ingest(self, spark):
        self.spark = spark
        company_list = self.get_company_list()
        # initial_data = ["", 0.0, 0.0, 0.0, 0.0, 0.0]
        # company_data = self.create_data_frame(initial_data)
        for company in company_list:
            data = self.prepare_company(company)
            if(data != None):
                shortening_full = list(data)[0]
                columns = StructType([StructField("open", StringType())\
                                    ,StructField("high", StringType())\
                                    ,StructField("low", StringType())\
                                    ,StructField("close", StringType())\
                                    ,StructField("adj close", StringType())])

                df = self.create_data_frame(data[shortening_full], columns)
                # df = self.spark.createDataFrame( data[shortening_full], schema=columns)

                df.show()
                self.save_raw_data(df, shortening_full)
                # new_df = df.union(company_data)
                # company_data = new_df

    def get_web_data(self, url):
        page = requests.get(url)
        return page.content

    def get_stock_data_from_web_source(self, shortening_full):
        response = requests.get("https://finance.yahoo.com/quote/"+shortening_full+"/history?p="+shortening_full)
        data = self.parse_yahoo_request(response.text)
        return data

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
        else:
            self.handle_dividendes(row)

    def handle_dividendes(self):
        print(row)
        print("TODO: handle_dividendes")

    def prepare_data(self, data, shortening_full):
        n = np.array(data)
        np.set_printoptions(formatter={'float_kind': self.float_formatter})
        n = n.astype(np.float)
        # self.save_raw_data(data, shortening_full)
        return self.prepare_object(n)

    def create_data_frame(self, result):
        columns = ["company", "max", "median", "last", "average", "std"]
        return self.spark.createDataFrame([(result)], schema=columns)

    def save_raw_data(self, input_data, shortening_full):
        input_file = "/data/stocks/" + shortening_full
        file_format = "csv"
        mode = "append"
        self.write_file(input_file, input_data, file_format, mode)
