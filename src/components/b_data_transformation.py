from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional    
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline
from scipy import stats
import numpy as np
from src.entities.config_entities import Datapreprocessor
from src.entities.builders import PysparkDTPipelineBuilder
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
import pandas as pd

class DataTransformationPipeline(PysparkDTPipelineBuilder):
    """
    A class to build a data transformation pipeline using PySpark.
    """

    def __init__(self, spark_session: Any):
        """
        Initialize the DataTransformationPipeline with a Spark session.
        """
        self.conf = pyspark.SparkConf().setAppName('appName').setMaster('local[*]').set('spark.driver.memory', '15g')
        self.sc = pyspark.SparkContext(conf=self.conf)
        self.spark = SparkSession(self.sc)
        self.data_preprocessor = Datapreprocessor()
        self.data_preprocessor.interim_datasets = None
        self.data_preprocessor.processed_datasets = None


    def reset(self) -> None:

        """
        Reset the builder to its initial state.
        """

        self.spark = SparkSession.builder.getOrCreate()
        self.data_preprocessor = Datapreprocessor()
        self.data_preprocessor.interim_datasets = []
        self.data_preprocessor.processed_datasets = []


    def aggregate_data(self) -> DataFrame:

        '''
        Aggregate data from multiple sources and perform joins to create a unified dataset.
        '''

        # Load data from Parquet files
        customer = self.spark.read.parquet("../data/raw/customer.parquet")
        catalog_sales = self.spark.read.parquet("../data/raw/catalog_sales.parquet")
        web_sales = self.spark.read.parquet("../data/raw/web_sales.parquet")
        store_sales = self.spark.read.parquet("../data/raw/store_sales.parquet")
        catalog_returns = self.spark.read.parquet("../data/raw/catalog_returns.parquet")
        web_returns = self.spark.read.parquet("../data/raw/web_returns.parquet")
        store_returns = self.spark.read.parquet("../data/raw/store_returns.parquet")
        household_demographics = self.spark.read.parquet("../data/raw/household_demographics.parquet")
        customer_demographics = self.spark.read.parquet("../data/raw/customer_demographics.parquet")
        customer_address = self.spark.read.parquet("../data/raw/customer_address.parquet")
        date_dim = self.spark.read.parquet("../data/raw/date_dim.parquet")

        # Build the CTE equivalent using DataFrame API
        result_df = customer.alias("c") \
            .join(
                catalog_sales.alias("cs"),
                (col("c.c_customer_sk") == col("cs.cs_ship_customer_sk")) &
                (col("c.c_customer_sk") == col("cs.cs_bill_customer_sk")),
                "left"
            ) \
            .join(
                web_sales.alias("ws"),
                (col("c.c_customer_sk") == col("ws.ws_ship_customer_sk")) &
                (col("c.c_customer_sk") == col("ws.ws_bill_customer_sk")),
                "left"
            ) \
            .join(
                store_sales.alias("ss"),
                col("c.c_customer_sk") == col("ss.ss_customer_sk"),
                "inner"
            ) \
            .join(
                catalog_returns.alias("cr"),
                (col("c.c_customer_sk") == col("cr.cr_returning_customer_sk")) &
                (col("c.c_customer_sk") == col("cr.cr_refunded_customer_sk")),
                "left"
            ) \
            .join(
                web_returns.alias("wr"),
                (col("c.c_customer_sk") == col("wr.wr_returning_customer_sk")) &
                (col("c.c_customer_sk") == col("wr.wr_refunded_customer_sk")),
                "left"
            ) \
            .join(
                store_returns.alias("sr"),
                col("c.c_customer_sk") == col("sr.sr_customer_sk"),
                "left"
            ) \
            .join(
                household_demographics.alias("hd"),
                col("c.c_current_hdemo_sk") == col("hd.hd_demo_sk"),
                "inner"
            ) \
            .join(
                customer_demographics.alias("cd"),
                col("c.c_current_cdemo_sk") == col("cd.cd_demo_sk"),
                "inner"
            ) \
            .join(
                customer_address.alias("ca"),
                col("c.c_current_addr_sk") == col("ca.ca_address_sk"),
                "inner"
            ) \
            .join(
                date_dim.alias("dd"),
                col("c.c_first_sales_date_sk") == col("dd.d_date_sk"),
                "inner"
            )

        # Show results
        return result_df


    def clean_data_remove_duplicates(self, data) -> DataFrame:
        '''
        Clean the data by removing duplicates and handling missing values.
        '''
        data = data.dropDuplicates()
        self.data_preprocessor.interim_datasets = data
        return data
    

    def interim_toParquet_toPandas(self, data, file_path):
        data.write_parquet(file_path)
        self.data_preprocessor.interim_datasets = pd.read_parquet(file_path)
        
        return True


    def feature_selection(self, data, method, params):
        '''
        Perform feature selection on the data using the specified method.
        '''
        


    def build(self) -> Pipeline:
        """
        Build the data transformation pipeline.
        """
        # Define the stages of the pipeline
        stages = [
            # Add your transformation stages here
        ]
        
        return Pipeline(stages=stages)