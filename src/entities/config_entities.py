from dataclasses import dataclass
from typing import Optional, Dict, Any

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

class Datapreprocessor:
    def __init__(self):
        self.stages = []
        interim_datasets = []
        processed_datasets = []


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

@dataclass
class DataIngestionConfig:
    database: DatabaseConfig
    output_dir: str
    file_format: str = "parquet"
    batch_size: Optional[int] = 10000
    aws_config: Optional[Dict[str, Any]] = None

@dataclass
class AWSConfig:
    region_name: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str