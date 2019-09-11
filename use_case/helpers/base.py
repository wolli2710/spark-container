import requests
from bs4 import BeautifulSoup
import numpy as np
from functools import reduce
from pyspark.sql import DataFrame

class Base():
    float_formatter = lambda self, x: "%.2f" % x

    def read_file(self, input_file, file_format='parquet', sep=',', encoding='utf-8'):
        if not (os.path.isfile(input_file)):
            return
        if file_format == 'parquet':
            return self.spark.read.parquet(input_file)
        elif file_format == 'csv':
            return self.spark.read.csv(input_file, header=True, sep=sep, encoding=encoding)

    def write_file(self, input_file, input_data, file_format, mode=None):
        if file_format == 'parquet':
            input_data.write.parquet(input_file+".csv", mode=mode)
        elif file_format == 'csv':
            input_data.write.csv(input_file+".csv", mode=mode, sep=';', header=True)

    def get_company_list(self):
        print("not implemented")
        return []

    def parse_request(self, html_doc):
        print("not implemented")

    def prepare_company(self, company):
        print("not implemented")

    def get_web_data(self, url):
        page = requests.get(url)
        return page.content

    def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)

    def process(self, spark):
        self.spark = spark
        company_list = self.get_company_list()
        initial_data = ["", 0.0, 0.0, 0.0, 0.0, 0.0]
        company_data = self.create_data_frame(initial_data)
        for company in company_list:
            data = self.prepare_company(company)
            if(data != None):
                df = self.prepare_dataframe(data)
                new_df = df.union(company_data)
                company_data = new_df
                company_data.show()
                self.predict_low_cost_high_value(company_data)
        return company_data

    def get_stock_data_from_web_source(self, shortage):
        response = requests.get("https://finance.yahoo.com/quote/"+shortage+"/history?p="+shortage)
        data = self.parse_yahoo_request(response.text)
        data_object = self.prepare_data(data)
        input_file = "/data/test.csv"
        file_format = "csv"
        mode = "append"
        return data_object

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

    def prepare_data(self, data):
        n = np.array(data)
        np.set_printoptions(formatter={'float_kind': self.float_formatter})
        n = n.astype(np.float)
        return self.prepare_object(n)

    def create_data_frame(self, result):
        columns = ["company", "max", "median", "last", "average", "std"]
        return self.spark.createDataFrame([(result)], schema=columns)

    def predict_low_cost_high_value(self, df):
        result = df.agg({"average": "max"}).collect()[0]
        print(result)

    def prepare_dataframe(self, data_list):
        threshold = 80.0
        for key, val in data_list.items():
            data = val
            result = [
                key,
                data["max"][0].item(),
                data["median"][0].item(),
                data["last"][0].item(),
                data["average"][0].item(),
                data["std"][0].item(),
            ]
        return self.create_data_frame(result)

    def prepare_object(self, data_array):
        average = np.average(data_array, axis=0)
        median = np.median(data_array, axis=0)
        max = np.max(data_array, axis=0)
        std = np.std(data_array, axis=0)
        last = data_array[-1]
        obj = {
            "average": average,
            "median": median,
            "max": max,
            "std": std,
            "last": last
        }
        return obj
