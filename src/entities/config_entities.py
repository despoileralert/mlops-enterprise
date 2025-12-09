from dataclasses import dataclass
from typing import Optional, Dict, Any

'''Data Ingestion Pipeline Entities Module'''

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


'''Data Preprocessor Entities'''


@dataclass
class PySparkConfig:
    app_name: str
    master: str
    driver_memory: str
    jdbc_url: Optional[str]
    jdbc_properties: Optional[Dict[str, str]]
    sql_file: str

@dataclass
class DataPreprocessingConfig:
    output_dir: str
    file_format: str = "parquet"
    spark_config: PySparkConfig 
    batch_size: Optional[int] = 10000
    aws_config: Optional[Dict[str, Any]] = None
    
@dataclass
class LocalDatabaseWriteConfig:
    jdbc_url: str
    sql_uri: str
    table: str
    cnx1host: str
    cnx1user: str
    cnx1password: str
    cnx1database: str
    cnx2host: str
    cnx2user: str
    cnx2password: str
    cnx2database: str
    sql_buildtable = str,
    sql_insertdata = str