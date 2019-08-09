

class VI:
    def __init__(self):
        print("vi")

    def bla():
        print("bla")

    def get_company_list(self, url):
        response = requests.get("https://www.wienerborse.at/marktdaten/aktien-sonstige/preisdaten/?ISIN=AT000AGRANA3")
        self.data = self.parse_request(response.content)

    def parse_request(self, html_doc):
        data = []
        soup = BeautifulSoup(html_doc, 'html.parser')
        table = soup.find(class_='csc-default')
        print(table.content)
