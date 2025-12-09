from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from mysql.connector import MySQLConnection, Error
from dotenv import load_dotenv
import sys, os, boto3, pyspark, re, mysql.connector
import pymysql
from sqlalchemy import create_engine
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import sklearn as sk
from sklearn.preprocessing import StandardScaler, LabelEncoder, OrdinalEncoder


from src.entities.config_entities import PySparkConfig, DataPreprocessingConfig, AWSConfig, LocalDatabaseWriteConfig
from src.utils.config_utils import create_data_preprocessing_config
from src.exception import tpcdsException
from src.logger import logging
        
        
class aggregatorCleaner:

    def __init__(self, locDbConfig: LocalDatabaseWriteConfig):
        self.config = locDbConfig
        self.connection: Optional[MySQLConnection] = None
        self.logger = logging.getLogger(__name__)
    
    def build_aggregate_step(self):
        try:
            with open('../src/sql/data_aggregation2.sql') as sqlquery:
                cnx = create_engine('mysql+pymysql://guest:ctu-relational@relational.fel.cvut.cz/tpcds')
                sqlquery = sqlquery.read()
                df = pd.read_sql(sqlquery, cnx) #read the entire table
            df.dropna(inplace = True)
            df.drop_duplicates()
            df.drop(columns={'c_email_address', 'c_birth_country', 'c_salutation', 'c_first_name', 'c_last_name', 'c_birth_country'}, inplace = True)
            df[['c_birth_year','c_birth_month','c_birth_day', 'store_transactions', 'catalog_transactions', 'web_transactions', 'total_return_quantity']] = df[['c_birth_year','c_birth_month','c_birth_day', 'store_transactions', 'catalog_transactions', 'web_transactions', 'total_return_quantity']].astype(int)
            df['birth_date'] = pd.to_datetime(df['c_birth_year'].astype(str) + '-' + df['c_birth_month'].astype(str) + '-' + df['c_birth_day'].astype(str))
            df.drop(columns = {'c_birth_month', 'c_birth_day', 'c_birth_year'}, inplace = True)

            cols = list(df.columns)
            cols = cols[:2] + [cols[-1]] + cols[3:-1]
            df = df[cols]
            return df
        
        except Exception as e:
            error_msg = f"Query execution failed: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())
        
    def cleaner_featureProcessingStep(self):
        try:
            df = pd.read_sql_table(con=self.config.sql_uri, table_name=self.config.table)

            df.drop(columns = ['c_customer_sk','hd_demo_sk','ca_city', 'ca_country', 'ca_state'], inplace = True)
            df.dropna(subset=['c_preferred_cust_flag'], inplace = True)
            df.drop_duplicates(inplace=True)

            df[df.columns[1:15]] = df[df.columns[1:15]].astype('category')
            df['active_channels_count'] = df['active_channels_count'].astype('category')
            df['c_customer_sk'] = df['c_customer_sk'].astype('object')

            numeric_cols = df.select_dtypes(include=['number', 'float64']).columns
            scaler = StandardScaler()
            df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
            df[numeric_cols]

            categorical_cols = list(df.select_dtypes(include=['category']).columns[5:10])
            categorical_cols += ['hd_buy_potential']
            print(categorical_cols)
            encoder = LabelEncoder()
            df[categorical_cols] = df[categorical_cols].apply(encoder.fit_transform).astype('category')
            return df
        
        except Exception as e:
            error_msg = f"Cleaning operation failed: {e}"
            self.logger.error(error_msg)
            raise tpcdsException(error_msg, sys.exc_info())



