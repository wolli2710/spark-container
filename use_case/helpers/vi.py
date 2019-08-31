from use_case.helpers.base import Base
from bs4 import BeautifulSoup

import os
import time

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
        # print(company_website)
        url = "https://www.wienerborse.at/marktdaten/aktien-sonstige/preisdaten/?ISIN=" + company
        shortening = self.get_web_data_with_element(url, "Kürzel")
        print(shortening)

    def get_web_data_with_element(self, url, element):
        self.company_data = []

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox') # required when running as root user. otherwise you would get no sandbox errors.

        # driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver', chrome_options=chrome_options, service_args=['--verbose', '--log-path=/tmp/chromedriver.log'])
        driver = webdriver.Chrome(executable_path='/opt/chrome/chromedriver', chrome_options=chrome_options, service_args=['--verbose', '--log-path=/tmp/chromedriver.log'])

        driver.get(url)
        driver.execute_script("window.scrollTo(0, 1200);")
        time.sleep(1)
        elem = driver.find_elements_by_xpath("//th[contains(text(), 'Kürzel')]/following-sibling::td")


        ele = elem[0].text #.encode('utf8')

        # WebElement element = driver.findElement(By.id("id_of_element"));

        # element = driver.find_elements_by_xpath("//th[contains(text(), 'Kürzel')]")
        # driver.executeScript("arguments[0].scrollIntoView(true);", element);
        # Thread.sleep(500);


        #Wait till Javascript is loaded
        # WebDriverWait(driver, delay).until(EC.presence_of_element_located( driver.find_elements_by_xpath("//th[contains(text(), '"+element+"')]") ) )
        # while True:
        #     try:
        #         print(driver.find_elements_by_xpath("//th[contains(text(), 'Kürzel')]"))
        #         print("Page is ready!")
        #         break # it will break from the loop once the specific element will be present.
        #     except:
        #         print("Loading took too much time!-Try again")

        return ele
