'''Data Ingestion Pipeline Entities Module'''

class SQLDataConnector:
    def __init__(self):
        self.user = None,    
        self.password = None,
        self.host = None,
        self.port = None,
        self.database = None,
        self.write_path = None ,
        self.write_mode = None



'''Data preprocessing pipeline entities'''

class DataPreprocessor:
    def __init__(self):
        self.rawdatasets: dict = {}
        self.processed_datasets: dict = {} 

