import requests

class Base():
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

    def process(self):
        company_list = self.get_company_list()
        company_data = []
        for company in company_list:
            company_data.append( self.prepare_company(company) )
        return company_data
