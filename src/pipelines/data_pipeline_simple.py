"""
Simplified Data Pipeline for MLOps Enterprise Platform

This is a demonstration version that shows the core architecture
without external dependencies like pandas, numpy, etc.
"""

import logging
import time
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum
import hashlib

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
    output_format: str = "json"
    output_path: Optional[str] = None
    schema_validation: bool = True
    data_quality_checks: bool = True


class SimpleDataFrame:
    """Simple data structure to replace pandas DataFrame."""
    
    def __init__(self, data: List[Dict[str, Any]], columns: Optional[List[str]] = None):
        self.data = data
        self.columns = columns or (list(data[0].keys()) if data else [])
    
    def __len__(self):
        return len(self.data)
    
    def copy(self):
        return SimpleDataFrame(self.data.copy(), self.columns.copy())
    
    def to_dict(self, orient='records'):
        return self.data
    
    def to_json(self, file_path: str, orient='records', indent=2):
        with open(file_path, 'w') as f:
            json.dump(self.data, f, indent=indent)
    
    def to_csv(self, file_path: str):
        if not self.data:
            return
        
        with open(file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(self.data)


class DataValidator:
    """Data validation and quality checking."""
    
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_schema(self, data: SimpleDataFrame, expected_schema: Dict[str, type]) -> bool:
        """Validate data schema against expected schema."""
        try:
            if not data.data:
                raise DataValidationError("Empty dataset")
            
            for column, expected_type in expected_schema.items():
                if column not in data.columns:
                    raise DataValidationError(f"Missing required column: {column}")
                
                # Check data types for first few rows
                for row in data.data[:10]:  # Sample first 10 rows
                    if column in row and not isinstance(row[column], expected_type):
                        raise DataValidationError(f"Column {column} has incorrect data type")
            
            self.logger.info("Schema validation passed")
            return True
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            raise DataValidationError(f"Schema validation failed: {str(e)}")
    
    def check_data_quality(self, data: SimpleDataFrame) -> Dict[str, Any]:
        """Perform data quality checks."""
        if not data.data:
            return {"total_rows": 0, "null_counts": {}, "duplicate_rows": 0}
        
        quality_metrics = {
            "total_rows": len(data.data),
            "null_counts": {},
            "duplicate_rows": 0,
            "memory_usage": len(str(data.data)),
            "column_types": {}
        }
        
        # Count nulls per column
        for column in data.columns:
            null_count = sum(1 for row in data.data if row.get(column) is None)
            quality_metrics["null_counts"][column] = null_count
        
        # Count duplicates
        seen = set()
        duplicates = 0
        for row in data.data:
            row_tuple = tuple(row.values())
            if row_tuple in seen:
                duplicates += 1
            else:
                seen.add(row_tuple)
        quality_metrics["duplicate_rows"] = duplicates
        
        # Column types
        for column in data.columns:
            if data.data:
                quality_metrics["column_types"][column] = type(data.data[0].get(column)).__name__
        
        self.logger.info(f"Data quality metrics: {quality_metrics}")
        return quality_metrics
    
    def validate_data(self, data: SimpleDataFrame, schema: Optional[Dict[str, type]] = None) -> Dict[str, Any]:
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
    
    def apply_transformations(self, data: SimpleDataFrame) -> SimpleDataFrame:
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
    
    def standard_transformations(self, data: SimpleDataFrame) -> SimpleDataFrame:
        """Apply standard data transformations."""
        if not data.data:
            return data
        
        # Remove duplicates
        seen = set()
        unique_data = []
        for row in data.data:
            row_tuple = tuple(row.values())
            if row_tuple not in seen:
                unique_data.append(row)
                seen.add(row_tuple)
        
        # Handle missing values
        for row in unique_data:
            for key in row:
                if row[key] is None:
                    if isinstance(row.get(key, 0), (int, float)):
                        row[key] = 0  # Default for numeric
                    else:
                        row[key] = "Unknown"  # Default for string
        
        # Create new DataFrame with processed data
        return SimpleDataFrame(unique_data, data.columns)


class DataIngestor:
    """Data ingestion from various sources."""
    
    def __init__(self, config: DataPipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.config_loader = ConfigLoader()
    
    @retry_with_backoff(max_retries=3, backoff_factor=2.0)
    def ingest_from_database(self, query: str) -> SimpleDataFrame:
        """Ingest data from database (mock implementation)."""
        try:
            # Mock database response
            mock_data = [
                {"id": 1, "name": "Alice", "amount": 100.0},
                {"id": 2, "name": "Bob", "amount": 200.0},
                {"id": 3, "name": "Charlie", "amount": 300.0}
            ]
            
            self.logger.info(f"Ingested {len(mock_data)} rows from database (mock)")
            return SimpleDataFrame(mock_data)
            
        except Exception as e:
            self.logger.error(f"Database ingestion failed: {str(e)}")
            raise PipelineError(f"Database ingestion failed: {str(e)}")
    
    def ingest_from_file(self, file_path: str) -> SimpleDataFrame:
        """Ingest data from file."""
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.suffix.lower() == '.csv':
                data = []
                with open(file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    data = list(reader)
            elif file_path.suffix.lower() == '.json':
                with open(file_path, 'r') as f:
                    data = json.load(f)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            self.logger.info(f"Ingested {len(data)} rows from file: {file_path}")
            return SimpleDataFrame(data)
            
        except Exception as e:
            self.logger.error(f"File ingestion failed: {str(e)}")
            raise PipelineError(f"File ingestion failed: {str(e)}")
    
    def ingest_from_api(self, api_url: str, params: Optional[Dict[str, Any]] = None) -> SimpleDataFrame:
        """Ingest data from API (mock implementation)."""
        try:
            # Mock API response
            mock_data = [
                {"user_id": 1, "score": 85, "status": "active"},
                {"user_id": 2, "score": 92, "status": "active"},
                {"user_id": 3, "score": 78, "status": "inactive"}
            ]
            
            self.logger.info(f"Ingested {len(mock_data)} rows from API: {api_url} (mock)")
            return SimpleDataFrame(mock_data)
            
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
    
    def _ingest_data(self, source_config: Dict[str, Any]) -> SimpleDataFrame:
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
        
        elif self.config.source_type == DataSourceType.API:
            api_url = source_config.get("api_url")
            if not api_url:
                raise ValueError("API source requires 'api_url' parameter")
            params = source_config.get("params", {})
            return self.ingestor.ingest_from_api(api_url, params)
        
        else:
            raise ValueError(f"Unsupported source type: {self.config.source_type}")
    
    def _store_data(self, data: SimpleDataFrame):
        """Store processed data."""
        output_path = Path(self.config.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.config.output_format.lower() == 'csv':
            data.to_csv(output_path)
        elif self.config.output_format.lower() == 'json':
            data.to_json(output_path)
        else:
            raise ValueError(f"Unsupported output format: {self.config.output_format}")
        
        self.logger.info(f"Data stored to: {output_path}")


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
        print("Usage: python data_pipeline_simple.py <source_type> <source_config> [output_path]")
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