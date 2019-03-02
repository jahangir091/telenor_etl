
from extract import Extract
from load import Load

class Transform:

    extracted_data = None

    def extract(self):
        ex = Extract()
        ex.connect()
        self.extracted_data = ex.subscriptions[0]

    def transform(self):
        pass

    def load(self):
        l = Load()
        l.connect()
        l.insert(self.extracted_data)

