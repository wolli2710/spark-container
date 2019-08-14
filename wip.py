from use_case.helpers.base import Base
from bs4 import BeautifulSoup

import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class VI(Base):
    def __init__(self):
        Base()
        print("vi")

    def get_company_list(self):
        data = self.get_web_data("https://www.wienerborse.at/emittenten/aktien/unternehmensliste/")
        return self.parse_company_list(data)

    def parse_company_list(self, content):
        tds = []
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.find(class_='panel-primary')
        table_rows = table.find_all('tr')

        for row in table_rows:
            result = row.find_all('td')
            if len(result) > 1:
                tds.append(result[0].getText())
        return tds

    # def parse_request(self, html_doc):
    #     data = []
    #     soup = BeautifulSoup(html_doc, 'html.parser')
    #     table = soup.find(class_='csc-default')
    #     print(table.content)

    def prepare_company(self, company):
        base_url = "https://www.wienerborse.at"
        url = "/marktdaten/aktien-sonstige/preisdaten/?ISIN=" + company
        company_website = self.get_web_data_with_element(base_url, url, "Kürzel")

    def get_web_data_with_element(self, base_url, url, element):
        self.company_data = []

        content = self.get_web_data(base_url + url)
        soup = BeautifulSoup(content, 'html.parser')

        result = soup.find_all('a', href=lambda href: href and url in href )

        content = self.get_web_data(base_url + result[0]['href'])

        print(content)


        return page
