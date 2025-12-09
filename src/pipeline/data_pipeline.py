import sys
from typing import List, Optional
from src.utils.config_utils import create_data_ingestion_config
from src.components.a_data_extraction import MySQLDataIngestionComponent
from src.exception import tpcdsException
from src.logger import logging

class DataIngestionPipeline:
    """Pipeline for orchestrating data ingestion process"""
    
    def __init__(self, config_path: str = "src/config/data_ingestion.yaml"):
        self.logger = logging.getLogger(__name__)
        try:
            self.config = create_data_ingestion_config(config_path)
            self.ingestion_component = MySQLDataIngestionComponent(self.config)
            self.logger.info("Data ingestion pipeline initialized successfully")
        except Exception as e:
            error_msg = f"Failed to initialize pipeline: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())
    
    def run(self, tables: Optional[List[str]] = None) -> List[str]:
        """Run the data ingestion pipeline"""
        try:
            self.logger.info("Starting data ingestion pipeline")
            
            if tables:
                # Extract specific tables
                extracted_files = []
                for table in tables:
                    file_path = self.ingestion_component.extract_table_data(table)
                    extracted_files.append(file_path)
            else:
                # Extract all tables
                extracted_files = self.ingestion_component.extract_all_tables()
            
            self.logger.info(f"Pipeline completed successfully. Extracted {len(extracted_files)} files")
            return extracted_files
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())

# For backward compatibility and easy execution
if __name__ == "__main__":
    pipeline = DataIngestionPipeline()
    results = pipeline.run()
    print(f"Extracted files: {results}")