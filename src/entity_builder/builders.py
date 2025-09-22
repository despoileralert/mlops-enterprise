from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from src.entity_builder.entities import *
from pyspark.sql import SparkSession, DataFrame

## Abstract base class for SQL Data Ingestion
class DIPipelineBuilder(ABC):

    @abstractmethod
    def reset(self) -> None:
        """
        Reset the builder to its initial state.
        """
        pass

    @abstractmethod
    def get_connection(self) -> Optional[Any]:
        """
        Get the connection object for the data source.

        :return: An optional connection object, or None if no connection is needed.
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return the results.

        :param query: The SQL query to execute.
        :return: A list of dictionaries representing the rows returned by the query.
        """
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """
        Close the connection to the data source.
        """
        pass
    
    @abstractmethod
    def write_asParquet(self, file_path: str, data: List[Dict[str, Any]]) -> None:
        """ 
        Write the data to a file at the specified path.
        """
        pass

    @abstractmethod
    def writeParquet_tos3(self, data: Any) -> None:
        """
        Write the data to a Parquet file.

        :param data: A list of dictionaries containing the data to be written.
        """
        pass

    @abstractmethod
    def build(self, data: List[Dict[str, Any]]) -> List[Any]:
        """
        Build a list of entities from the provided data.

        :param data: A list of dictionaries containing the data to be processed.
        :return: A list of Entity objects.
        """
        pass



#Abstract base class for pyspark data processing

class PysparkDTPipelineBuilder(ABC):
    
    def __init__(self, spark_session: Any):
        """
        Initialize the PysparkPipelineBuilder with a Spark session.

        :param spark_session: The Spark session to use for data processing.
        """
        self.spark_session = spark_session
    

    @abstractmethod
    def reset(self) -> None:
        """
        Reset the builder to its initial state.
        """
        pass
    

    @abstractmethod
    def get_from_s3(self, bucket_name: str, file_name: str) -> Any:
        """
        Get data from an S3 bucket.

        :param bucket_name: The name of the S3 bucket.
        :param file_name: The name of the file to retrieve from the S3 bucket.
        :return: The data retrieved from the S3 bucket.
        """
        pass
    

    @abstractmethod
    def aggregate_data(self) -> DataFrame:
        """
        Aggregate the data based on specified groupings and aggregation functions.

        :param data: A list of dictionaries containing the data to be aggregated.
        :param group_by: A list of keys to group the data by.
        :param aggregations: A dictionary specifying the aggregation functions to apply.
        :return: A list of dictionaries with the aggregated data.
        """
        pass


    @abstractmethod
    def feature_selection(self, data: DataFrame, method: str, params: Dict[str, Any]) -> DataFrame:
        """
        Perform feature selection on the data using the specified method.

        :param data: A list of dictionaries containing the data to be processed.
        :param method: The feature selection method to use (e.g., "PCA", "Chi-Squared").
        :param params: A dictionary of parameters specific to the chosen method.
        :return: A list of dictionaries with the selected features.
        """
        pass
    

    @abstractmethod
    def clean_data_remove_duplicates(self, data: List[Dict[str, Any]]) -> DataFrame:
        """
        Clean the data by removing duplicates.

        :param data: A list of dictionaries containing the data to be cleaned.
        :return: A list of dictionaries with duplicates removed.
        """
        pass


    @abstractmethod
    def clean_data_remove_nulls(self, data: List[Dict[str, Any]]) -> DataFrame:
        """
        Clean the data by removing null values.

        :param data: A list of dictionaries containing the data to be cleaned.
        :return: A list of dictionaries with null values removed.
        """
        pass


    @abstractmethod
    def clean_data_remove_outliers(self, data: List[Dict[str, Any]]) -> DataFrame:
        """
        Clean the data by removing outliers.

        :param data: A list of dictionaries containing the data to be cleaned.
        :return: A list of dictionaries with outliers removed.
        """
        pass


    @abstractmethod
    def transform_data_normalization(self, data: List[Dict[str, Any]]) -> DataFrame:
        """
        Transform the data by normalizing it.

        :param data: A list of dictionaries containing the data to be transformed.
        :return: A list of dictionaries with normalized data.
        """
        pass    


    @abstractmethod
    def transform_data_standardization(self, data: List[Dict[str, Any]]) -> DataFrame:
        """
        Transform the data by standardizing it.

        :param data: A list of dictionaries containing the data to be transformed.
        :return: A list of dictionaries with standardized data.
        """
        pass


    @abstractmethod
    def transform_data_encoding(self, data: List[Dict[str, Any]]) -> DataFrame:
        """
        Transform the data by encoding categorical variables.

        :param data: A list of dictionaries containing the data to be transformed.
        :return: A list of dictionaries with encoded data.
        """
        pass    


    @abstractmethod
    def interim_toparquet(self, data: List[Dict[str, Any]], file_path: str) -> Any:
        """
        Write the data to a Parquet file.

        :param data: A list of dictionaries containing the data to be written.
        :param file_path: The path where the Parquet file will be saved.
        """
        pass


    @abstractmethod
    def parquet_to_s3(self, data: List[Dict[str, Any]], bucket_name: str, file_name: str) -> Any:
        """
        Write the data to an S3 bucket.

        :param data: A list of dictionaries containing the data to be written.
        :param bucket_name: The name of the S3 bucket.
        :param file_name: The name of the file to be saved in the S3 bucket.
        """
        pass


    @abstractmethod
    def build(self, data: List[Dict[str, Any]]) -> List[Any]:
        """
        Build a list of entities from the provided data.

        :param data: A list of dictionaries containing the data to be processed.
        :return: A list of Entity objects.
        """
        pass