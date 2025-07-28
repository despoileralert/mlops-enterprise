"""
Data Pipeline for MLOps Enterprise Platform

This module provides a comprehensive data processing pipeline that integrates with
the existing MLOps infrastructure. It supports data ingestion, validation, 
transformation, and storage with monitoring and error handling.

Features:
- Multi-source data ingestion (databases, files, APIs)
- Data validation and quality checks
- Configurable transformations
- Monitoring and alerting
- Error handling and retry logic
- Integration with MLflow for experiment tracking
- Support for both batch and streaming processing
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pyspark.sql as spark_sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import mlflow
from mlflow.tracking import MlflowClient

# Local imports
from utils.config_loader import ConfigLoader, Environment
from utils.logging_utils import setup_logger
from utils.exception_handler import PipelineError, DataValidationError
from utils.helpers import timing_decorator, retry_with_backoff, generate_hash


class DataSourceType(str, Enum):
    """Supported data source types."""
    DATABASE = "database"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    API = "api"
    STREAM = "stream"


class ProcessingMode(str, Enum):
    """Data processing modes."""
    BATCH = "batch"
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"


@dataclass
class DataPipelineConfig:
    """Configuration for data pipeline."""
    source_type: DataSourceType
    source_config: Dict[str, Any]
    processing_mode: ProcessingMode = ProcessingMode.BATCH
    validation_enabled: bool = True
    transformation_enabled: bool = True
    monitoring_enabled: bool = True
    batch_size: int = 10000
    max_retries: int = 3
    timeout_seconds: int = 300
    output_format: str = "parquet"
    output_path: Optional[str] = None
    schema_validation: bool = True
    data_quality_checks: bool = True


class DataValidator:
    """Data validation and quality checking."""
    
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_schema(self, data: pd.DataFrame, expected_schema: Dict[str, type]) -> bool:
        """Validate data schema against expected schema."""
        try:
            for column, expected_type in expected_schema.items():
                if column not in data.columns:
                    raise DataValidationError(f"Missing required column: {column}")
                
                # Check data types
                if not all(isinstance(val, expected_type) for val in data[column].dropna()):
                    raise DataValidationError(f"Column {column} has incorrect data type")
            
            self.logger.info("Schema validation passed")
            return True
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            raise DataValidationError(f"Schema validation failed: {str(e)}")
    
    def check_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Perform data quality checks."""
        quality_metrics = {
            "total_rows": len(data),
            "null_counts": data.isnull().sum().to_dict(),
            "duplicate_rows": data.duplicated().sum(),
            "memory_usage": data.memory_usage(deep=True).sum(),
            "column_types": data.dtypes.to_dict()
        }
        
        # Additional quality checks
        quality_metrics.update({
            "empty_columns": [col for col in data.columns if data[col].isnull().all()],
            "constant_columns": [col for col in data.columns if data[col].nunique() <= 1],
            "high_cardinality": [col for col in data.columns if data[col].nunique() > data.shape[0] * 0.8]
        })
        
        self.logger.info(f"Data quality metrics: {quality_metrics}")
        return quality_metrics
    
    def validate_data(self, data: pd.DataFrame, schema: Optional[Dict[str, type]] = None) -> Dict[str, Any]:
        """Comprehensive data validation."""
        validation_results = {}
        
        if self.config.schema_validation and schema:
            validation_results["schema_valid"] = self.validate_schema(data, schema)
        
        if self.config.data_quality_checks:
            validation_results["quality_metrics"] = self.check_data_quality(data)
        
        return validation_results


class DataTransformer:
    """Data transformation and feature engineering."""
    
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.transformations: List[Callable] = []
    
    def add_transformation(self, transformation_func: Callable):
        """Add a transformation function to the pipeline."""
        self.transformations.append(transformation_func)
        self.logger.info(f"Added transformation: {transformation_func.__name__}")
    
    def apply_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply all registered transformations to the data."""
        if not self.transformations:
            self.logger.warning("No transformations registered")
            return data
        
        transformed_data = data.copy()
        
        for i, transformation in enumerate(self.transformations):
            try:
                start_time = time.time()
                transformed_data = transformation(transformed_data)
                duration = time.time() - start_time
                self.logger.info(f"Applied transformation {i+1}/{len(self.transformations)}: "
                               f"{transformation.__name__} in {duration:.2f}s")
            except Exception as e:
                self.logger.error(f"Transformation {transformation.__name__} failed: {str(e)}")
                raise PipelineError(f"Transformation failed: {str(e)}")
        
        return transformed_data
    
    def standard_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply standard data transformations."""
        # Remove duplicates
        data = data.drop_duplicates()
        
        # Handle missing values
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        data[numeric_columns] = data[numeric_columns].fillna(data[numeric_columns].median())
        
        # Handle categorical columns
        categorical_columns = data.select_dtypes(include=['object']).columns
        data[categorical_columns] = data[categorical_columns].fillna('Unknown')
        
        # Convert date columns
        date_columns = data.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            data[f'{col}_year'] = data[col].dt.year
            data[f'{col}_month'] = data[col].dt.month
            data[f'{col}_day'] = data[col].dt.day
        
        return data


class DataIngestor:
    """Data ingestion from various sources."""
    
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.config_loader = ConfigLoader()
    
    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def ingest_from_database(self, query: str) -> pd.DataFrame:
        """Ingest data from database."""
        try:
            db_config = self.config_loader.get_database_config()
            connection_string = (
                f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            
            engine = create_engine(connection_string)
            
            with engine.connect() as connection:
                data = pd.read_sql(query, connection)
            
            self.logger.info(f"Ingested {len(data)} rows from database")
            return data
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database ingestion failed: {str(e)}")
            raise PipelineError(f"Database ingestion failed: {str(e)}")
    
    def ingest_from_file(self, file_path: str) -> pd.DataFrame:
        """Ingest data from file."""
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.suffix.lower() == '.csv':
                data = pd.read_csv(file_path)
            elif file_path.suffix.lower() == '.json':
                data = pd.read_json(file_path)
            elif file_path.suffix.lower() == '.parquet':
                data = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            self.logger.info(f"Ingested {len(data)} rows from file: {file_path}")
            return data
            
        except Exception as e:
            self.logger.error(f"File ingestion failed: {str(e)}")
            raise PipelineError(f"File ingestion failed: {str(e)}")
    
    def ingest_from_api(self, api_url: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Ingest data from API."""
        try:
            import requests
            
            response = requests.get(api_url, params=params, timeout=self.config.timeout_seconds)
            response.raise_for_status()
            
            data = pd.DataFrame(response.json())
            self.logger.info(f"Ingested {len(data)} rows from API: {api_url}")
            return data
            
        except Exception as e:
            self.logger.error(f"API ingestion failed: {str(e)}")
            raise PipelineError(f"API ingestion failed: {str(e)}")


class DataPipeline:
    """
    Main data pipeline orchestrator.
    
    This class coordinates the entire data processing pipeline including
    ingestion, validation, transformation, and storage.
    """
    
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.logger = setup_logger(__name__)
        self.validator = DataValidator(config)
        self.transformer = DataTransformer(config)
        self.ingestor = DataIngestor(config)
        
        # Initialize MLflow
        self.mlflow_client = None
        if self.config.monitoring_enabled:
            self._setup_mlflow()
    
    def _setup_mlflow(self):
        """Setup MLflow for experiment tracking."""
        try:
            config_loader = ConfigLoader()
            mlflow_config = config_loader.get_mlflow_config()
            
            mlflow.set_tracking_uri(mlflow_config['tracking_uri'])
            mlflow.set_experiment(mlflow_config['experiment_name'])
            
            self.mlflow_client = MlflowClient()
            self.logger.info("MLflow setup completed")
            
        except Exception as e:
            self.logger.warning(f"MLflow setup failed: {str(e)}")
    
    @timing_decorator
    def run_pipeline(self, source_config: Dict[str, Any], 
                    schema: Optional[Dict[str, type]] = None) -> Dict[str, Any]:
        """
        Execute the complete data pipeline.
        
        Args:
            source_config: Configuration for data source
            schema: Expected data schema for validation
            
        Returns:
            Dictionary containing pipeline results and metrics
        """
        pipeline_start = time.time()
        results = {
            "pipeline_start": datetime.now().isoformat(),
            "config": self.config.__dict__,
            "metrics": {}
        }
        
        try:
            # Step 1: Data Ingestion
            self.logger.info("Starting data ingestion...")
            data = self._ingest_data(source_config)
            results["metrics"]["ingestion_rows"] = len(data)
            
            # Step 2: Data Validation
            if self.config.validation_enabled:
                self.logger.info("Starting data validation...")
                validation_results = self.validator.validate_data(data, schema)
                results["validation"] = validation_results
            
            # Step 3: Data Transformation
            if self.config.transformation_enabled:
                self.logger.info("Starting data transformation...")
                data = self.transformer.apply_transformations(data)
                data = self.transformer.standard_transformations(data)
                results["metrics"]["transformed_rows"] = len(data)
            
            # Step 4: Data Storage
            if self.config.output_path:
                self.logger.info("Starting data storage...")
                self._store_data(data)
                results["storage"] = {"output_path": self.config.output_path}
            
            # Step 5: Log to MLflow
            if self.config.monitoring_enabled and self.mlflow_client:
                self._log_to_mlflow(results, data)
            
            pipeline_duration = time.time() - pipeline_start
            results["pipeline_duration"] = pipeline_duration
            results["status"] = "success"
            
            self.logger.info(f"Pipeline completed successfully in {pipeline_duration:.2f}s")
            
        except Exception as e:
            pipeline_duration = time.time() - pipeline_start
            results["pipeline_duration"] = pipeline_duration
            results["status"] = "failed"
            results["error"] = str(e)
            
            self.logger.error(f"Pipeline failed after {pipeline_duration:.2f}s: {str(e)}")
            raise PipelineError(f"Pipeline execution failed: {str(e)}")
        
        return results
    
    def _ingest_data(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """Ingest data based on source configuration."""
        if self.config.source_type == DataSourceType.DATABASE:
            query = source_config.get("query")
            if not query:
                raise ValueError("Database source requires 'query' parameter")
            return self.ingestor.ingest_from_database(query)
        
        elif self.config.source_type == DataSourceType.CSV:
            file_path = source_config.get("file_path")
            if not file_path:
                raise ValueError("CSV source requires 'file_path' parameter")
            return self.ingestor.ingest_from_file(file_path)
        
        elif self.config.source_type == DataSourceType.JSON:
            file_path = source_config.get("file_path")
            if not file_path:
                raise ValueError("JSON source requires 'file_path' parameter")
            return self.ingestor.ingest_from_file(file_path)
        
        elif self.config.source_type == DataSourceType.PARQUET:
            file_path = source_config.get("file_path")
            if not file_path:
                raise ValueError("Parquet source requires 'file_path' parameter")
            return self.ingestor.ingest_from_file(file_path)
        
        elif self.config.source_type == DataSourceType.API:
            api_url = source_config.get("api_url")
            if not api_url:
                raise ValueError("API source requires 'api_url' parameter")
            params = source_config.get("params", {})
            return self.ingestor.ingest_from_api(api_url, params)
        
        else:
            raise ValueError(f"Unsupported source type: {self.config.source_type}")
    
    def _store_data(self, data: pd.DataFrame):
        """Store processed data."""
        output_path = Path(self.config.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.config.output_format.lower() == 'csv':
            data.to_csv(output_path, index=False)
        elif self.config.output_format.lower() == 'parquet':
            data.to_parquet(output_path, index=False)
        elif self.config.output_format.lower() == 'json':
            data.to_json(output_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported output format: {self.config.output_format}")
        
        self.logger.info(f"Data stored to: {output_path}")
    
    def _log_to_mlflow(self, results: Dict[str, Any], data: pd.DataFrame):
        """Log pipeline results to MLflow."""
        try:
            with mlflow.start_run(run_name=f"data_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                # Log parameters
                mlflow.log_params({
                    "source_type": self.config.source_type.value,
                    "processing_mode": self.config.processing_mode.value,
                    "batch_size": self.config.batch_size,
                    "validation_enabled": self.config.validation_enabled,
                    "transformation_enabled": self.config.transformation_enabled
                })
                
                # Log metrics
                mlflow.log_metrics({
                    "pipeline_duration": results["pipeline_duration"],
                    "ingestion_rows": results["metrics"]["ingestion_rows"],
                    "transformed_rows": results["metrics"].get("transformed_rows", 0),
                    "data_memory_mb": data.memory_usage(deep=True).sum() / 1024 / 1024
                })
                
                # Log artifacts
                if self.config.output_path:
                    mlflow.log_artifact(self.config.output_path)
                
                # Log data profile
                profile_path = f"/tmp/data_profile_{generate_hash(str(data.shape))}.json"
                data.describe().to_json(profile_path)
                mlflow.log_artifact(profile_path)
                
        except Exception as e:
            self.logger.warning(f"Failed to log to MLflow: {str(e)}")


def create_data_pipeline(source_type: DataSourceType,
                       source_config: Dict[str, Any],
                       output_path: Optional[str] = None,
                       **kwargs) -> DataPipeline:
    """
    Factory function to create a data pipeline with common configurations.
    
    Args:
        source_type: Type of data source
        source_config: Configuration for the data source
        output_path: Path for output data
        **kwargs: Additional configuration parameters
        
    Returns:
        Configured DataPipeline instance
    """
    config = DataPipelineConfig(
        source_type=source_type,
        source_config=source_config,
        output_path=output_path,
        **kwargs
    )
    
    return DataPipeline(config)


# Example usage and utility functions
def run_database_pipeline(query: str, output_path: str, **kwargs) -> Dict[str, Any]:
    """Run a database-to-file pipeline."""
    pipeline = create_data_pipeline(
        source_type=DataSourceType.DATABASE,
        source_config={"query": query},
        output_path=output_path,
        **kwargs
    )
    
    return pipeline.run_pipeline({"query": query})


def run_file_pipeline(input_path: str, output_path: str, **kwargs) -> Dict[str, Any]:
    """Run a file-to-file pipeline."""
    source_type = DataSourceType.CSV
    if input_path.endswith('.json'):
        source_type = DataSourceType.JSON
    elif input_path.endswith('.parquet'):
        source_type = DataSourceType.PARQUET
    
    pipeline = create_data_pipeline(
        source_type=source_type,
        source_config={"file_path": input_path},
        output_path=output_path,
        **kwargs
    )
    
    return pipeline.run_pipeline({"file_path": input_path})


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python data_pipeline.py <source_type> <source_config> [output_path]")
        sys.exit(1)
    
    source_type = DataSourceType(sys.argv[1])
    source_config = eval(sys.argv[2])  # In production, use proper JSON parsing
    output_path = sys.argv[3] if len(sys.argv) > 3 else None
    
    pipeline = create_data_pipeline(
        source_type=source_type,
        source_config=source_config,
        output_path=output_path
    )
    
    results = pipeline.run_pipeline(source_config)
    print(f"Pipeline completed: {results}")