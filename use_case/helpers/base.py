import requests
from bs4 import BeautifulSoup
import numpy as np
from functools import reduce
from pyspark.sql import DataFrame
import os
from pyspark.sql import functions as F
import pandas as pd
import pandas_ta as ta

class Base():
    float_formatter = lambda self, x: "%.2f" % x

    def process(self, spark, source):
        self.spark = spark
        self.source = source
        company_list = self.get_company_list()

        for company in company_list:
            shortening = self.get_shortening(company)
            if(shortening != None):
                df = self.load_company(shortening)
                print(shortening)
                # if(df != None):
                if isinstance(df, pd.DataFrame):
                    # numpy_array = self.prepare_data(df)
                    result_df = self.prepare_dataframe(df)
                    print(result_df)
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
        # if file_format == 'parquet':
        #     return self.spark.read.parquet(input_file)
        elif file_format == 'csv':
            # return self.spark.read.csv(input_file, header=True, sep=sep, encoding=encoding)
            return pd.read_csv(input_file, sep=sep, encoding=encoding)

    def write_file(self, input_file, input_data, file_format, mode=None):
        print("file write")
        if file_format == 'csv':
            input_data.to_csv(input_file+".csv", sep=';', encoding='utf-8')

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

    def prepare_rows(self, row, data):
        if len(row) >= 4:
            data.insert(0, [ele.replace(",", "").replace("-", "0") for ele in row[0:-1] if ele])
        else:
            self.handle_dividendes(row)

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
        last1 = df.iloc[-1]
        last2 = df.iloc[-2]
        print("******************************")
        # print(last['close'].item())
        print( last1['close'] )
        print( last2['close'] )
        print(df.ta.sma())
        print(df.ta.ema())
        print(df.sort_values('timestamp')[df['timestamp']>1476397546].ta.ema())
        print("******************************")

        # return df.agg(F.max('close'), F.min('close'), F.avg('close'), F.stddev('close') )
        return df.sort_values('timestamp')[df['timestamp']>1476397546].agg({"close":['max','min', 'mean', 'median', 'std'] })
