from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional    
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline
from src.entity_builder.entities import DataPreprocessor
from src.entity_builder.builders import PysparkDTPipelineBuilder

class DataTransformationPipeline(PysparkDTPipelineBuilder):
    """
    A class to build a data transformation pipeline using PySpark.
    """

    def __init__(self, spark_session: Any):
        """
        Initialize the DataTransformationPipeline with a Spark session.
        """
        super().__init__(spark_session)
        self.spark = spark_session

    def reset(self) -> None:
        """
        Reset the builder to its initial state.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.data_preprocessor = DataPreprocessor()
        self.data_preprocessor.rawdatasets = {}
        self.data_preprocessor.processed_datasets = {}

    def get_from_s3(self, bucket_name: str, file_name: str) -> DataFrame:
        """
        Get data from an S3 bucket.

        :param bucket_name: The name of the S3 bucket.
        :param file_name: The name of the file to retrieve from the S3 bucket.
        :return: The data retrieved from the S3 bucket.
        """
        pass

    def build(self) -> Pipeline:
        """
        Build the data transformation pipeline.
        """
        # Define the stages of the pipeline
        stages = [
            # Add your transformation stages here
        ]
        
        return Pipeline(stages=stages)