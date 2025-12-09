import yaml
import os
from typing import TypeVar, Type, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from src.entities.config_entities import DatabaseConfig, DataIngestionConfig, AWSConfig, PySparkConfig, DataPreprocessingConfig, LocalDatabaseWriteConfig

T = TypeVar('T')

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file with environment variable substitution"""
    load_dotenv()
    with open(config_path, 'r') as file:
        content = file.read()
        
    # Replace environment variables
    for key, value in os.environ.items():
        content = content.replace(f"${{{key}}}", value)
    
    return yaml.safe_load(content)


def create_data_ingestion_config(config_path: str = "src/config/data_ingestion.yaml") -> DataIngestionConfig:
    """Create DataIngestionConfig from YAML file"""
    config_dict = load_config(config_path)
    
    db_config = DatabaseConfig(**config_dict['database'])
    aws_config = None
    if all([
        os.getenv('AWS_S3_REGION_NAME'),
        os.getenv('AWS_S3_ACCESS_KEY_ID'),
        os.getenv('AWS_S3_SECRET_ACCESS_KEY')
    ]):
        aws_config = {
            'region_name': os.getenv('AWS_S3_REGION_NAME'),
            'access_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY'),
            'bucket_name': config_dict['aws']['bucket_name']
        }
    
    return DataIngestionConfig(
        database=db_config,
        output_dir=config_dict['data_ingestion']['output_dir'],
        file_format=config_dict['data_ingestion']['file_format'],
        batch_size=config_dict['data_ingestion'].get('batch_size', 10000),
        aws_config=aws_config
    )


def create_data_preprocessing_config(config_path: str = "src/config/data_preprocessing.yaml") -> LocalDatabaseWriteConfig:
    """Create DataPreprocessingConfig from YAML file"""
    config_dict = load_config(config_path)
    
    aws_config = None
    if all([
        os.getenv('AWS_S3_REGION_NAME'),
        os.getenv('AWS_S3_ACCESS_KEY_ID'),
        os.getenv('AWS_S3_SECRET_ACCESS_KEY')
    ]):
        aws_config = {
            'region_name': os.getenv('AWS_S3_REGION_NAME'),
            'access_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY'),
            'bucket_name': config_dict['aws']['bucket_name']
        }
    passw = os.getenv('DATABASE_PASSWORD')
    
    return LocalDatabaseWriteConfig(
        jdbc_url = config_dict['localdbconfig']['jdbc_url'], #"jdbc:mysql://localhost:3306/mlops-tpcds-semi"
        table = config_dict['localdbconfig']['table'],
        cnx1host = config_dict['localdbconfig']['cnx1host'],
        cnx1user = config_dict['localdbconfig']['cnx1user'],
        cnx1password = config_dict['localdbconfig']['cnx1password'],
        cnx1database = config_dict['localdbconfig']['cnx1database'],
        cnx2host = config_dict['localdbconfig']['cnx2host'],
        cnx2user = config_dict['localdbconfig']['cnx2user'],
        cnx2password = passw,
        cnx2database = config_dict['localdbconfig']['cnx2database'],
        sql_buildtable = "src/sql/data_aggregation.sql",
        sql_insertdata = "src/sql/build_table.sql"
    )