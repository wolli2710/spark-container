import requests
from bs4 import BeautifulSoup
import numpy as np
from functools import reduce
from pyspark.sql import DataFrame
import os
from pyspark.sql import functions as F


class Base():
    float_formatter = lambda self, x: "%.2f" % x

    def process(self, spark, source):
        self.spark = spark
        self.source = source
        company_list = self.get_company_list()

        for company in company_list:
            shortening = self.get_shortening(company)
            print(shortening)
            if(shortening != None):
                df = self.load_company(shortening)
                if(df != None):
                    # numpy_array = self.prepare_data(df)
                    result_df = self.prepare_dataframe(df)

                    print(shortening)
                    result_df.show()
                    # self.predict_low_cost_high_value(df)

    def load_company(self, company):
        format = "csv"
        file_path = self.source + company + "." + format
        df = self.read_file(input_file=file_path, file_format=format)
        return df

    def read_file(self, input_file, file_format='parquet', sep=';', encoding='utf-8'):
        if not (os.path.isfile(input_file) or os.path.isdir(input_file)):
            print("File not found")
            return None
        if file_format == 'parquet':
            return self.spark.read.parquet(input_file)
        elif file_format == 'csv':
            return self.spark.read.csv(input_file, header=True, sep=sep, encoding=encoding)

    def write_file(self, input_file, input_data, file_format, mode=None):
        if file_format == 'parquet':
            input_data.write.parquet(input_file+".parquet", mode=mode)
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

    def get_stock_data_from_web_source(self, shortening_full):
        response = requests.get("https://finance.yahoo.com/quote/"+shortening_full+"/history?p="+shortening_full)
        data = self.parse_yahoo_request(response.text)
        # return normal array
        return data
        # return numpy array
        return self.prepare_data(data, shortening_full)

    def prepare_data(self, data):
        n = np.array(data)
        np.set_printoptions(formatter={'float_kind': self.float_formatter})
        raw_data = n.astype(np.float)
        # self.save_raw_data(data, shortening_full)
        # return self.prepare_object(n)
        return raw_data

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
            data.insert(0, [ele.replace(",", "").replace("-", "0") for ele in row[0:-1] if ele])
        else:
            self.handle_dividendes(row)

    def handle_dividendes(self):
        print(row)
        print("TODO: handle_dividendes")

    def create_data_frame(self, data, schema=None):
        if schema != None:
            columns = schema
        else:
            columns = ["company", "max", "median", "last", "average", "std"]
        return self.spark.createDataFrame(data, schema=columns)

    def predict_low_cost_high_value(self, df):
        result = df.orderBy(["last", "max", "std"], ascending=[0,0,1]).collect()
        print(result)

    def prepare_dataframe(self, df):
        # last_elem = df.limit(1).filter(df["open"])
        # last_elem.show()

        return df.agg(F.max('open'), F.min('open'), F.avg('open'), F.stddev('open') )

    # def prepare_dataframe(self, data_list):
    #     for key, val in data_list.items():
    #         data = val
    #         result = [
    #             key,
    #             data["max"][0].item(),
    #             data["median"][0].item(),
    #             data["last"][0].item(),
    #             data["average"][0].item(),
    #             data["std"][0].item(),
    #         ]
    #     return self.create_data_frame(result)

    def prepare_aggregation_results(self, data_array):
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
