import mysql.connector
from mysql.connector import MySQLConnection, Error
from typing import Any, Dict, List, Optional, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import boto3
import sys
from pathlib import Path

from src.entities.config_entities import DataIngestionConfig, DatabaseConfig, AWSConfig
from src.exception import tpcdsException
from src.logger import logging

class DatabaseConnector:
    """Handles database connection and query execution"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.config = db_config
        self.connection: Optional[MySQLConnection] = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> MySQLConnection:
        """Establish database connection"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connection = mysql.connector.connect(
                    user=self.config.user,
                    password=self.config.password,
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    use_pure=True
                )
                self.logger.info(f"Connected to database: {self.config.database}")
            return self.connection
        except mysql.connector.Error as err:
            error_msg = f"Database connection failed: {err}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())
    
    def execute_query(self, query: str) -> List[Tuple]:
        """Execute SQL query and return results"""
        try:
            connection = self.connect()
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            self.logger.debug(f"Query executed successfully: {query[:50]}...")
            return results
        except Exception as e:
            error_msg = f"Query execution failed: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())
    
    def get_table_schema(self, table_name: str) -> List[Tuple]:
        """Get table schema information"""
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{self.config.database}'
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query)
    
    def get_all_tables(self) -> List[str]:
        """Get all table names from database"""
        results = self.execute_query("SHOW TABLES")
        return [table[0] for table in results]
    
    def close(self) -> None:
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

class ParquetWriter:
    """Handles writing data to Parquet files"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # Type conversion mapping
        self.type_mapping = {
            'int': pa.int64(),
            'float': pa.float64(),
            'str': pa.string(),
            'datetime': pa.timestamp('ms'),
            'date': pa.date32(),
            'boolean': pa.bool_(),
            'decimal': pa.decimal128(38, 18),
            'char': pa.string(),
            'varchar': pa.string(),
            'text': pa.string(),
            'tinyint': pa.int8(),
            'smallint': pa.int16(),
            'mediumint': pa.int32(),
            'bigint': pa.int64(),
            'blob': pa.binary(),
            'enum': pa.string(),
            'set': pa.string(),
            'json': pa.string(),
            'geometry': pa.binary(),
            'point': pa.binary()
        }
    
    def write_table(self, data: List[Tuple], schema: List[Tuple], table_name: str) -> str:
        """Write table data to Parquet file"""
        try:
            # Extract column names and types
            columns = [col[0] for col in schema]
            column_types = [col[1] for col in schema]
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # Create PyArrow schema
            arrow_schema = pa.schema([
                pa.field(col_name, self.type_mapping.get(col_type, pa.string()))
                for col_name, col_type in zip(columns, column_types)
            ])
            
            # Convert to PyArrow table
            table = pa.Table.from_pandas(df, schema=arrow_schema)
            
            # Write to file
            file_path = self.output_dir / f"{table_name}.parquet"
            pq.write_table(table, file_path)
            
            self.logger.info(f"Written {len(data)} rows to {file_path}")
            return str(file_path)
            
        except Exception as e:
            error_msg = f"Failed to write parquet file for table {table_name}: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())

class S3Uploader:
    """Handles uploading files to S3"""
    
    def __init__(self, aws_config: Dict[str, Any]):
        self.config = aws_config
        self.logger = logging.getLogger(__name__)
        self.s3_resource = None
    
    def _get_s3_resource(self):
        """Get S3 resource with lazy initialization"""
        if not self.s3_resource:
            self.s3_resource = boto3.resource(
                service_name='s3',
                region_name=self.config['region_name'],
                aws_access_key_id=self.config['access_key_id'],
                aws_secret_access_key=self.config['secret_access_key']
            )
        return self.s3_resource
    
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """Upload file to S3"""
        try:
            s3 = self._get_s3_resource()
            bucket = s3.Bucket(self.config['bucket_name'])
            bucket.upload_file(Filename=local_path, Key=s3_key)
            self.logger.info(f"Uploaded {local_path} to s3://{self.config['bucket_name']}/{s3_key}")
            return True
        except Exception as e:
            error_msg = f"S3 upload failed for {local_path}: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())

class MySQLDataIngestionComponent:
    """Main component for MySQL data ingestion"""
    
    def __init__(self, config: DataIngestionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize sub-components
        self.db_connector = DatabaseConnector(config.database)
        self.parquet_writer = ParquetWriter(config.output_dir)
        self.s3_uploader = S3Uploader(config.aws_config) if config.aws_config else None
    
    def extract_table_data(self, table_name: str) -> str:
        """Extract data from a single table"""
        try:
            self.logger.info(f"Starting extraction for table: {table_name}")
            
            # Get table schema
            schema = self.db_connector.get_table_schema(table_name)
            if not schema:
                raise ValueError(f"No schema found for table: {table_name}")
            
            # Extract data
            data = self.db_connector.execute_query(f"SELECT * FROM {table_name}")
            
            # Write to Parquet
            file_path = self.parquet_writer.write_table(data, schema, table_name)
            
            # Upload to S3 if configured
            if self.s3_uploader:
                s3_key = f"raw/{table_name}.parquet"
                self.s3_uploader.upload_file(file_path, s3_key)
            
            self.logger.info(f"Successfully extracted {len(data)} rows from {table_name}")
            return file_path
            
        except Exception as e:
            error_msg = f"Failed to extract data from table {table_name}: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())
    
    def extract_all_tables(self) -> List[str]:
        """Extract data from all tables"""
        try:
            tables = self.db_connector.get_all_tables()
            extracted_files = []
            
            self.logger.info(f"Found {len(tables)} tables to extract")
            
            for table in tables:
                file_path = self.extract_table_data(table)
                extracted_files.append(file_path)
            
            return extracted_files
            
        except Exception as e:
            error_msg = f"Failed to extract all tables: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())
        finally:
            self.db_connector.close()