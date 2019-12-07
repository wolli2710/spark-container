from use_case.helpers.base import Base
from use_case.helpers.base_ingest import BaseIngest
from bs4 import BeautifulSoup
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class VI(Base, BaseIngest):
    def __init__(self):
        Base()
        BaseIngest()
        print("vi")
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox') # required when running as root user. otherwise you would get no sandbox errors.
        self.driver = webdriver.Chrome(executable_path='/opt/chrome/chromedriver', chrome_options=chrome_options, service_args=['--verbose', '--log-path=/tmp/chromedriver.log'])

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

    def get_shortening(self, company):
        url = "https://www.wienerborse.at/marktdaten/aktien-sonstige/preisdaten/?ISIN=" + company
        shortening = self.get_web_data_with_element(url, "Kürzel")
        try:
            shortening = self.get_web_data_with_element(url, "Kürzel")
            return shortening + ".VI"
        except:
            print("get web data exception")
            return None

    def prepare_company(self, company):
        shortening = self.get_shortening(company)
        if(shortening != None):
            try:
                if(shortening != None):
                    self.get_stock_data_from_web_source(shortening)
                else:
                    print("Error on company:" + company)
                    return None
            except:
                print("Connection Exception")

    def get_web_data_with_element(self, url, element):
        self.company_data = []
        self.driver.get(url)
        self.driver.execute_script("window.scrollTo(0, 1200);")
        time.sleep(1)
        elem = self.driver.find_elements_by_xpath("//th[contains(text(), 'Kürzel')]/following-sibling::td")
        if(len(elem) > 0):
            ele = elem[0].text
            return ele
        return None
