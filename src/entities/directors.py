from src.entities.config_entities import SQLDataConnector
from src.entities.builders import DIBuilder
from src.components.a_data_extraction import MySqlDataIngestion
import mysql.connector
from mysql.connector import MySQLConnection

class DataIngestionDirector:
    def __init__(self, builder):
        self.builder = builder

    def build_data_ingestion_pipeline(self) -> MySqlDataIngestion:
        """
        Build the data ingestion pipeline by executing the query and writing the results to a Parquet file.
        """
        return self.builder.build()
    
