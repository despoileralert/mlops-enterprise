"""
Tests for the Data Pipeline module.

This module contains comprehensive tests for the data pipeline functionality
including data ingestion, validation, transformation, and storage.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch

# Add src to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.data_pipeline import (
    DataPipeline,
    DataPipelineConfig,
    DataSourceType,
    ProcessingMode,
    DataValidator,
    DataTransformer,
    DataIngestor,
    create_data_pipeline,
    run_file_pipeline
)
from utils.exception_handler import PipelineError, DataValidationError


class TestDataPipelineConfig:
    """Test DataPipelineConfig class."""
    
    def test_config_creation(self):
        """Test creating a DataPipelineConfig instance."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={"file_path": "test.csv"},
            output_path="output.parquet"
        )
        
        assert config.source_type == DataSourceType.CSV
        assert config.source_config == {"file_path": "test.csv"}
        assert config.output_path == "output.parquet"
        assert config.validation_enabled is True
        assert config.transformation_enabled is True
    
    def test_config_defaults(self):
        """Test DataPipelineConfig default values."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={}
        )
        
        assert config.processing_mode == ProcessingMode.BATCH
        assert config.batch_size == 10000
        assert config.max_retries == 3
        assert config.timeout_seconds == 300


class TestDataValidator:
    """Test DataValidator class."""
    
    def setup_method(self):
        """Setup test data."""
        self.config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={}
        )
        self.validator = DataValidator(self.config)
        
        # Create test data
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'amount': [100.0, 200.0, 300.0],
            'category': ['A', 'B', 'C'],
            'date': pd.date_range('2024-01-01', periods=3)
        })
    
    def test_validate_schema_success(self):
        """Test successful schema validation."""
        schema = {
            'id': int,
            'amount': float,
            'category': str
        }
        
        result = self.validator.validate_schema(self.test_data, schema)
        assert result is True
    
    def test_validate_schema_missing_column(self):
        """Test schema validation with missing column."""
        schema = {
            'id': int,
            'missing_column': str
        }
        
        with pytest.raises(DataValidationError):
            self.validator.validate_schema(self.test_data, schema)
    
    def test_validate_schema_wrong_type(self):
        """Test schema validation with wrong data type."""
        schema = {
            'id': str,  # Should be int
            'amount': float,
            'category': str
        }
        
        with pytest.raises(DataValidationError):
            self.validator.validate_schema(self.test_data, schema)
    
    def test_check_data_quality(self):
        """Test data quality checks."""
        quality_metrics = self.validator.check_data_quality(self.test_data)
        
        assert 'total_rows' in quality_metrics
        assert 'null_counts' in quality_metrics
        assert 'duplicate_rows' in quality_metrics
        assert 'memory_usage' in quality_metrics
        assert 'column_types' in quality_metrics
        
        assert quality_metrics['total_rows'] == 3
        assert quality_metrics['duplicate_rows'] == 0
    
    def test_validate_data_comprehensive(self):
        """Test comprehensive data validation."""
        schema = {
            'id': int,
            'amount': float,
            'category': str
        }
        
        results = self.validator.validate_data(self.test_data, schema)
        
        assert 'schema_valid' in results
        assert 'quality_metrics' in results
        assert results['schema_valid'] is True


class TestDataTransformer:
    """Test DataTransformer class."""
    
    def setup_method(self):
        """Setup test data."""
        self.config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={}
        )
        self.transformer = DataTransformer(self.config)
        
        # Create test data with missing values
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'amount': [100.0, np.nan, 300.0, 400.0],
            'category': ['A', 'B', np.nan, 'D'],
            'date': pd.date_range('2024-01-01', periods=4)
        })
    
    def test_add_transformation(self):
        """Test adding custom transformation."""
        def custom_transform(df):
            df['new_column'] = df['id'] * 2
            return df
        
        self.transformer.add_transformation(custom_transform)
        assert len(self.transformer.transformations) == 1
    
    def test_apply_transformations(self):
        """Test applying transformations."""
        def custom_transform(df):
            df['doubled_id'] = df['id'] * 2
            return df
        
        self.transformer.add_transformation(custom_transform)
        
        result = self.transformer.apply_transformations(self.test_data)
        
        assert 'doubled_id' in result.columns
        assert result['doubled_id'].iloc[0] == 2
    
    def test_standard_transformations(self):
        """Test standard transformations."""
        # Add some duplicates
        data_with_duplicates = pd.concat([self.test_data, self.test_data.iloc[0:1]])
        
        result = self.transformer.standard_transformations(data_with_duplicates)
        
        # Check that duplicates are removed
        assert len(result) == len(self.test_data)
        
        # Check that missing values are handled
        assert not result['amount'].isnull().any()
        assert not result['category'].isnull().any()
        
        # Check that date features are created
        assert 'date_year' in result.columns
        assert 'date_month' in result.columns
        assert 'date_day' in result.columns


class TestDataIngestor:
    """Test DataIngestor class."""
    
    def setup_method(self):
        """Setup test data."""
        self.config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={}
        )
        self.ingestor = DataIngestor(self.config)
    
    def test_ingest_from_file_csv(self):
        """Test CSV file ingestion."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,value\n1,Alice,100\n2,Bob,200\n")
            temp_file = f.name
        
        try:
            result = self.ingestor.ingest_from_file(temp_file)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == ['id', 'name', 'value']
            
        finally:
            # Cleanup
            Path(temp_file).unlink()
    
    def test_ingest_from_file_json(self):
        """Test JSON file ingestion."""
        # Create temporary JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]')
            temp_file = f.name
        
        try:
            result = self.ingestor.ingest_from_file(temp_file)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == ['id', 'name']
            
        finally:
            # Cleanup
            Path(temp_file).unlink()
    
    def test_ingest_from_file_not_found(self):
        """Test file ingestion with non-existent file."""
        with pytest.raises(PipelineError):
            self.ingestor.ingest_from_file("non_existent_file.csv")
    
    def test_ingest_from_file_unsupported_format(self):
        """Test file ingestion with unsupported format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("test data")
            temp_file = f.name
        
        try:
            with pytest.raises(PipelineError):
                self.ingestor.ingest_from_file(temp_file)
        finally:
            Path(temp_file).unlink()


class TestDataPipeline:
    """Test DataPipeline class."""
    
    def setup_method(self):
        """Setup test data."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_file = self.temp_dir / "test_input.csv"
        self.output_file = self.temp_dir / "test_output.parquet"
        
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'amount': [100.0, 200.0, 300.0],
            'category': ['A', 'B', 'C']
        })
        test_data.to_csv(self.input_file, index=False)
    
    def teardown_method(self):
        """Cleanup test files."""
        shutil.rmtree(self.temp_dir)
    
    def test_pipeline_creation(self):
        """Test creating a DataPipeline instance."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={"file_path": str(self.input_file)},
            output_path=str(self.output_file)
        )
        
        pipeline = DataPipeline(config)
        
        assert pipeline.config.source_type == DataSourceType.CSV
        assert pipeline.validator is not None
        assert pipeline.transformer is not None
        assert pipeline.ingestor is not None
    
    def test_pipeline_run_success(self):
        """Test successful pipeline run."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={"file_path": str(self.input_file)},
            output_path=str(self.output_file),
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False  # Disable MLflow for testing
        )
        
        pipeline = DataPipeline(config)
        
        results = pipeline.run_pipeline({"file_path": str(self.input_file)})
        
        assert results['status'] == 'success'
        assert 'pipeline_duration' in results
        assert 'metrics' in results
        assert results['metrics']['ingestion_rows'] == 3
        
        # Check that output file was created
        assert self.output_file.exists()
    
    def test_pipeline_run_with_schema_validation(self):
        """Test pipeline run with schema validation."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={"file_path": str(self.input_file)},
            output_path=str(self.output_file),
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        pipeline = DataPipeline(config)
        
        schema = {
            'id': int,
            'amount': float,
            'category': str
        }
        
        results = pipeline.run_pipeline(
            {"file_path": str(self.input_file)},
            schema=schema
        )
        
        assert results['status'] == 'success'
        assert 'validation' in results
        assert results['validation']['schema_valid'] is True
    
    def test_pipeline_run_with_custom_transformation(self):
        """Test pipeline run with custom transformation."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={"file_path": str(self.input_file)},
            output_path=str(self.output_file),
            validation_enabled=False,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        pipeline = DataPipeline(config)
        
        def custom_transform(df):
            df['doubled_amount'] = df['amount'] * 2
            return df
        
        pipeline.transformer.add_transformation(custom_transform)
        
        results = pipeline.run_pipeline({"file_path": str(self.input_file)})
        
        assert results['status'] == 'success'
        
        # Check that transformation was applied
        output_data = pd.read_parquet(self.output_file)
        assert 'doubled_amount' in output_data.columns
        assert output_data['doubled_amount'].iloc[0] == 200.0
    
    def test_pipeline_run_failure(self):
        """Test pipeline run with invalid source."""
        config = DataPipelineConfig(
            source_type=DataSourceType.CSV,
            source_config={"file_path": "non_existent_file.csv"},
            output_path=str(self.output_file)
        )
        
        pipeline = DataPipeline(config)
        
        with pytest.raises(PipelineError):
            pipeline.run_pipeline({"file_path": "non_existent_file.csv"})


class TestFactoryFunctions:
    """Test factory functions."""
    
    def setup_method(self):
        """Setup test data."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_file = self.temp_dir / "test_input.csv"
        self.output_file = self.temp_dir / "test_output.parquet"
        
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'amount': [100.0, 200.0, 300.0],
            'category': ['A', 'B', 'C']
        })
        test_data.to_csv(self.input_file, index=False)
    
    def teardown_method(self):
        """Cleanup test files."""
        shutil.rmtree(self.temp_dir)
    
    def test_create_data_pipeline(self):
        """Test create_data_pipeline factory function."""
        pipeline = create_data_pipeline(
            source_type=DataSourceType.CSV,
            source_config={"file_path": str(self.input_file)},
            output_path=str(self.output_file)
        )
        
        assert isinstance(pipeline, DataPipeline)
        assert pipeline.config.source_type == DataSourceType.CSV
        assert pipeline.config.output_path == str(self.output_file)
    
    def test_run_file_pipeline(self):
        """Test run_file_pipeline function."""
        results = run_file_pipeline(
            input_path=str(self.input_file),
            output_path=str(self.output_file),
            monitoring_enabled=False
        )
        
        assert results['status'] == 'success'
        assert self.output_file.exists()


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])