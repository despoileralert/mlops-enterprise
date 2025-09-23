from src.entity_builder.entities import SQLDataConnector
from src.entity_builder.builders import DIPipelineBuilder
from src.components.a_data_extraction import MySqlDataIngestionPipeline
import mysql.connector
from mysql.connector import MySQLConnection

class DataIngestionDirector:
    def __init__(self, builder):
        self.builder = builder

    def build_data_ingestion_pipeline(self) -> MySqlDataIngestionPipeline:
        """
        Build the data ingestion pipeline by executing the query and writing the results to a Parquet file.
        """
        return self.builder.build()
    
