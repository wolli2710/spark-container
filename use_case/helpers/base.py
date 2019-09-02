import requests
from bs4 import BeautifulSoup
import numpy as np

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

    def process(self, spark):
        self.spark = spark
        company_list = self.get_company_list()
        company_data = []
        for company in company_list:
            company_data.append( self.prepare_company(company) )
        print(company_data)
        return company_data

    def get_stock_data_from_web_source(self, shortage):
        response = requests.get("https://finance.yahoo.com/quote/"+shortage+"/history?p="+shortage)
        data = self.parse_yahoo_request(response.text)
        data_object = self.prepare_data(data)

        input_file = "/data/test.csv"
        # input_data = self.spark.createDataFrame([("nix")], ["a"])

        # input_data = self.spark.createDataFrame(data_object, ["average", "median", "max", "std", "last"])
        file_format = "csv"
        mode = "append"

        # self.write_file(input_file, input_data, file_format, mode)
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
        #TODO export raw data

        return self.prepare_object(n)

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
