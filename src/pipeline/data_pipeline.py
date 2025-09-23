import sys
from src.entity_builder.directors import DataIngestionDirector
from src.entity_builder.builders import DIPipelineBuilder
from src.components.a_data_extraction import MySqlDataIngestionPipeline

# Initialize the builder with the SQL data connector
builder = MySqlDataIngestionPipeline(DIPipelineBuilder)
director = DataIngestionDirector(builder)

# Build the data ingestion pipeline
pipeline = director.build_data_ingestion_pipeline()
