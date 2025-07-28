# Data Pipeline Documentation

## Overview

The Data Pipeline is a comprehensive data processing solution designed for the MLOps Enterprise Platform. It provides a robust, scalable, and configurable framework for data ingestion, validation, transformation, and storage with built-in monitoring and error handling.

## Features

- **Multi-Source Ingestion**: Support for databases, files (CSV, JSON, Parquet), APIs, and streaming data
- **Data Validation**: Schema validation, data quality checks, and type validation
- **Data Transformation**: Configurable transformations with support for custom functions
- **Monitoring & Logging**: Integration with MLflow for experiment tracking and metrics
- **Error Handling**: Retry logic, error recovery, and comprehensive logging
- **Performance Optimization**: Memory management, parallel processing, and caching
- **Configuration Management**: Environment-specific configurations with validation

## Architecture

The data pipeline consists of several key components:

```
DataPipeline
├── DataIngestor (Data Source Connectors)
├── DataValidator (Validation & Quality Checks)
├── DataTransformer (Data Processing & Feature Engineering)
└── Monitoring & Logging (MLflow Integration)
```

## Quick Start

### Basic Usage

```python
from src.pipelines.data_pipeline import create_data_pipeline, DataSourceType

# Create a simple file processing pipeline
pipeline = create_data_pipeline(
    source_type=DataSourceType.CSV,
    source_config={"file_path": "data/input.csv"},
    output_path="data/output.parquet"
)

# Run the pipeline
results = pipeline.run_pipeline({"file_path": "data/input.csv"})
print(f"Pipeline completed: {results}")
```

### Database Pipeline

```python
from src.pipelines.data_pipeline import run_database_pipeline

# Extract data from database
results = run_database_pipeline(
    query="SELECT * FROM transactions WHERE date >= '2024-01-01'",
    output_path="data/processed/transactions.parquet",
    validation_enabled=True,
    transformation_enabled=True
)
```

### File Processing Pipeline

```python
from src.pipelines.data_pipeline import run_file_pipeline

# Process CSV file
results = run_file_pipeline(
    input_path="data/raw/data.csv",
    output_path="data/processed/data.parquet",
    validation_enabled=True,
    transformation_enabled=True
)
```

## Configuration

### Pipeline Configuration

The pipeline can be configured using the `DataPipelineConfig` class:

```python
from src.pipelines.data_pipeline import DataPipelineConfig, DataSourceType, ProcessingMode

config = DataPipelineConfig(
    source_type=DataSourceType.CSV,
    source_config={"file_path": "data/input.csv"},
    processing_mode=ProcessingMode.BATCH,
    validation_enabled=True,
    transformation_enabled=True,
    monitoring_enabled=True,
    batch_size=10000,
    max_retries=3,
    timeout_seconds=300,
    output_format="parquet",
    output_path="data/output.parquet",
    schema_validation=True,
    data_quality_checks=True
)
```

### Configuration File

You can also use the YAML configuration file (`config/data_pipeline.yaml`):

```yaml
default:
  processing_mode: "batch"
  validation_enabled: true
  transformation_enabled: true
  monitoring_enabled: true
  batch_size: 10000
  max_retries: 3
  timeout_seconds: 300
  output_format: "parquet"
  schema_validation: true
  data_quality_checks: true
```

## Data Sources

### Database Source

```python
# MySQL/PostgreSQL connection
pipeline = create_data_pipeline(
    source_type=DataSourceType.DATABASE,
    source_config={"query": "SELECT * FROM table_name"},
    output_path="data/output.parquet"
)
```

### File Sources

```python
# CSV file
pipeline = create_data_pipeline(
    source_type=DataSourceType.CSV,
    source_config={"file_path": "data/input.csv"},
    output_path="data/output.parquet"
)

# JSON file
pipeline = create_data_pipeline(
    source_type=DataSourceType.JSON,
    source_config={"file_path": "data/input.json"},
    output_path="data/output.parquet"
)

# Parquet file
pipeline = create_data_pipeline(
    source_type=DataSourceType.PARQUET,
    source_config={"file_path": "data/input.parquet"},
    output_path="data/output.parquet"
)
```

### API Source

```python
# REST API
pipeline = create_data_pipeline(
    source_type=DataSourceType.API,
    source_config={
        "api_url": "https://api.example.com/data",
        "params": {"limit": 1000}
    },
    output_path="data/output.parquet"
)
```

## Data Validation

### Schema Validation

```python
# Define expected schema
expected_schema = {
    'customer_id': int,
    'amount': float,
    'transaction_date': str,
    'category': str
}

# Run pipeline with schema validation
results = pipeline.run_pipeline(
    source_config={"file_path": "data/input.csv"},
    schema=expected_schema
)
```

### Data Quality Checks

The pipeline automatically performs quality checks:

- Null value analysis
- Duplicate detection
- Data type validation
- Memory usage tracking
- Column cardinality analysis

## Data Transformation

### Standard Transformations

The pipeline includes standard transformations:

- Remove duplicates
- Handle missing values (median for numeric, 'Unknown' for categorical)
- Date feature extraction (year, month, day)
- Data type conversions

### Custom Transformations

```python
def custom_transformation(df):
    """Add custom features."""
    df['total_amount'] = df['amount'] * df['quantity']
    df['is_high_value'] = df['amount'] > 1000
    return df

# Add custom transformation
pipeline.transformer.add_transformation(custom_transformation)

# Run pipeline
results = pipeline.run_pipeline(source_config)
```

## Monitoring and Logging

### MLflow Integration

The pipeline automatically logs to MLflow:

- Pipeline parameters
- Performance metrics
- Data quality metrics
- Output artifacts

### Logging Configuration

```python
from src.utils.logging_utils import setup_logger

# Setup logging
logger = setup_logger(__name__)
logger.info("Pipeline started")
```

## Error Handling

### Retry Logic

The pipeline includes automatic retry logic for transient failures:

```python
@retry_with_backoff(max_retries=3, backoff_factor=2.0)
def database_ingestion(query):
    # Database connection with retry
    pass
```

### Error Recovery

- Graceful failure handling
- Error logging and reporting
- Failed data preservation
- Pipeline status tracking

## Performance Optimization

### Memory Management

```python
config = DataPipelineConfig(
    batch_size=10000,  # Process data in chunks
    memory_limit="4GB"  # Memory limit
)
```

### Parallel Processing

```python
config = DataPipelineConfig(
    parallel_processing=True,
    num_workers=4
)
```

## Environment-Specific Configuration

### Development Environment

```yaml
environments:
  dev:
    batch_size: 1000
    monitoring_enabled: false
    output_path: "data/dev/processed/"
```

### Production Environment

```yaml
environments:
  prod:
    batch_size: 10000
    monitoring_enabled: true
    output_path: "data/prod/processed/"
    alerts_enabled: true
```

## Examples

### Complete Example

```python
from src.pipelines.data_pipeline import create_data_pipeline, DataSourceType

def process_transaction_data():
    """Complete data processing pipeline."""
    
    # Create pipeline
    pipeline = create_data_pipeline(
        source_type=DataSourceType.CSV,
        source_config={"file_path": "data/raw/transactions.csv"},
        output_path="data/processed/transactions_processed.parquet",
        validation_enabled=True,
        transformation_enabled=True,
        monitoring_enabled=True
    )
    
    # Add custom transformation
    def add_fraud_features(df):
        df['amount_category'] = pd.cut(df['amount'], bins=5, labels=['low', 'medium', 'high'])
        df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour
        return df
    
    pipeline.transformer.add_transformation(add_fraud_features)
    
    # Define schema
    schema = {
        'transaction_id': int,
        'amount': float,
        'timestamp': str,
        'customer_id': int
    }
    
    # Run pipeline
    results = pipeline.run_pipeline(
        source_config={"file_path": "data/raw/transactions.csv"},
        schema=schema
    )
    
    return results

# Run the pipeline
results = process_transaction_data()
print(f"Pipeline completed: {results}")
```

### Command Line Usage

```bash
# Process CSV file
python src/pipelines/data_pipeline.py csv "{'file_path': 'data/input.csv'}" data/output.parquet

# Process database query
python src/pipelines/data_pipeline.py database "{'query': 'SELECT * FROM transactions'}" data/output.parquet
```

## Best Practices

### 1. Data Validation

- Always enable schema validation for production pipelines
- Define clear data quality thresholds
- Monitor data drift over time

### 2. Error Handling

- Implement proper error recovery mechanisms
- Log all errors with sufficient context
- Set up alerts for critical failures

### 3. Performance

- Use appropriate batch sizes for your data volume
- Enable parallel processing for large datasets
- Monitor memory usage and optimize accordingly

### 4. Monitoring

- Enable MLflow tracking for all production pipelines
- Set up alerts for pipeline failures
- Monitor data quality metrics over time

### 5. Configuration Management

- Use environment-specific configurations
- Validate all configuration parameters
- Document configuration changes

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Check database credentials in `config/database.yaml`
   - Verify network connectivity
   - Check firewall settings

2. **Memory Issues**
   - Reduce batch size
   - Enable data chunking
   - Monitor memory usage

3. **Validation Failures**
   - Review data schema
   - Check data quality metrics
   - Validate data types

4. **MLflow Connection Issues**
   - Verify MLflow server is running
   - Check tracking URI configuration
   - Ensure proper authentication

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## API Reference

### Classes

- `DataPipeline`: Main pipeline orchestrator
- `DataPipelineConfig`: Configuration management
- `DataValidator`: Data validation and quality checks
- `DataTransformer`: Data transformation and feature engineering
- `DataIngestor`: Data source connectors

### Functions

- `create_data_pipeline()`: Factory function for creating pipelines
- `run_database_pipeline()`: Quick database-to-file pipeline
- `run_file_pipeline()`: Quick file-to-file pipeline

### Enums

- `DataSourceType`: Supported data source types
- `ProcessingMode`: Data processing modes

## Contributing

When contributing to the data pipeline:

1. Follow the existing code structure
2. Add comprehensive tests
3. Update documentation
4. Follow the project's coding standards
5. Add type hints for all functions

## License

This data pipeline is part of the MLOps Enterprise Platform and follows the same licensing terms.