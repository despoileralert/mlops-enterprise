import yaml
import os
from typing import TypeVar, Type, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from src.entities.config_entities import DatabaseConfig, DataIngestionConfig, AWSConfig

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

