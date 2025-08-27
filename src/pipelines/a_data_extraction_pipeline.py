from src.entity_builder.entities import SQLDataConnector
from src.entity_builder.builders import DIPipelineBuilder
import mysql.connector
from mysql.connector import MySQLConnection, Error, cursor
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

class MySqlDataIngestionPipeline(DIPipelineBuilder):

    """Concrete implementation of the DIPipelineBuilder for MySQL data ingestion."""

    def __init__(self, builder: DIPipelineBuilder):
        self.connector = SQLDataConnector()


    def set_config(self):
        """Initialize the SQLDataConnector with default values."""
        
        self.connector.user = 'guest'
        self.connector.password = 'ctu-relational'
        self.connector.host = 'relational.fel.cvut.cz'
        self.connector.port = 3306
        self.connector.database = 'tpcds'
        self.connector.write_path = f"data/raw/testingingest.parquet"  # Default write path
        self.connector.write_mode = 'w'  # Default write mode
        self.connection_state = False
        self.cnxn: Optional[MySQLConnection] = None


    def reset(self) -> None:
        """Reset the builder to its initial state."""
        self.connection = None


    def get_connection(self) -> MySQLConnection:
        """Get the connection object for the data source."""
        if not self.connection_state:
            try:
                self.cnxn = mysql.connector.connect(
                    user=self.connector.user,
                    password=self.connector.password,
                    host=self.connector.host,
                    database=self.connector.database,
                    use_pure = True
                )
                self.connection_state = True
            except mysql.connector.Error as err:
                print(f"Error: {err}")
                return None
        return self.cnxn


    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return the results.
           Most probably ill do my data cleaning here as well. But mainly to write to paraquet file so I can use pyspark to read it later on."""
        
        cursor = self.cnxn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()  # Fetch results as dictionaries
        cursor.close()
        return results


    def close_connection(self) -> None:
        """Close the connection to the data source."""

        if self.connection_state:
            self.connection.close()
            self.connection = None
    

    def write_asParquet(self, file_path: str, data: List[Dict[str, Any]], tname: str) -> None:
        """Write the data to a file at the specified path."""

        typeconvert = {
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

        #df = pd.DataFrame(data)
        #table = pa.Table.from_pandas(df)
        cols_arr = []
        self.get_connection()
        cursor = self.cnxn.cursor()
        print(f"name of table: {tname[0]}, tableschema: {self.connector.database}")
        cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tname[0]}' AND TABLE_SCHEMA = '{self.connector.database}';")
        result = cursor.fetchall()
        for j in range(len(result)):
            cols_arr.append( [(result[j])[3], (result[j])[7]] )  # (column_name, data_type)
        print(cols_arr)
        df = pd.DataFrame(data, columns=[col[0] for col in cols_arr])
        table = pa.Table.from_pandas(df, schema=pa.schema([pa.field(col[0], typeconvert[col[1]]) for col in cols_arr]))
        print(f"Writing data to Parquet file at {file_path}")
        pq.write_table(table, self.connector.write_path)


    def writeParquet_tos3(self, file: Any) -> None:
        s3 = boto3.resource(
            service_name='s3',
            region_name = os.getenv('AWS_S3_REGION_NAME'),
            aws_access_key_id = os.getenv('AWS_S3_ACCESS_KEY_ID'),
            aws_secret_access_key = os.getenv('AWS_S3_SECRET_ACCESS_KEY')
        )

        s3.Bucket('euleong-databucket').upload_file(Filename = file, Key = file)
        pass


    def build(self) -> List[Any]:
        """Build a list of entities from the provided data."""

        # Placeholder for entity building logic
        self.set_config()
        self.get_connection()
        table_namelist = self.execute_query("SHOW TABLES")
        print("Tables in the database:", table_namelist)

        for table_name in table_namelist:
            print(f"Processing table: {table_name[0]}")
            self.connector.write_path = f"data/raw/{table_name[0]}.parquet"
            self.connector.write_mode = 'w'
            # Here you can add logic to process each table as needed
            data = self.execute_query(f"SELECT * FROM {table_name[0]}")
            self.write_asParquet(self.connector.write_path, data, table_name)
            self.writeParquet_tos3(f'data/raw/{table_name[0]}.parquet'), 
        
        return True
    