#!/usr/bin/env python3
"""
Data Pipeline Usage Examples

This script demonstrates how to use the data pipeline with different
data sources and configurations.
"""

import sys
import os
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.data_pipeline import (
    DataPipeline, 
    DataPipelineConfig, 
    DataSourceType, 
    ProcessingMode,
    create_data_pipeline,
    run_database_pipeline,
    run_file_pipeline
)
from utils.config_loader import ConfigLoader


def example_database_pipeline():
    """Example: Extract data from database and save to file."""
    print("=== Database Pipeline Example ===")
    
    # Configuration
    query = "SELECT * FROM transactions LIMIT 1000"
    output_path = "data/processed/database_output.parquet"
    
    try:
        # Run the pipeline
        results = run_database_pipeline(
            query=query,
            output_path=output_path,
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=True
        )
        
        print(f"Pipeline completed successfully!")
        print(f"Results: {results}")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")


def example_file_pipeline():
    """Example: Process CSV file and save to Parquet."""
    print("\n=== File Pipeline Example ===")
    
    # Create sample CSV file for demonstration
    import pandas as pd
    import numpy as np
    
    # Generate sample data
    sample_data = pd.DataFrame({
        'customer_id': range(1, 1001),
        'amount': np.random.normal(100, 50, 1000),
        'transaction_date': pd.date_range('2024-01-01', periods=1000, freq='H'),
        'category': np.random.choice(['food', 'transport', 'entertainment'], 1000),
        'fraud_flag': np.random.choice([0, 1], 1000, p=[0.95, 0.05])
    })
    
    # Save sample data
    input_path = "data/raw/sample_transactions.csv"
    output_path = "data/processed/sample_transactions_processed.parquet"
    
    # Ensure directory exists
    Path(input_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    sample_data.to_csv(input_path, index=False)
    print(f"Created sample data file: {input_path}")
    
    try:
        # Run the pipeline
        results = run_file_pipeline(
            input_path=input_path,
            output_path=output_path,
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=True
        )
        
        print(f"Pipeline completed successfully!")
        print(f"Results: {results}")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")


def example_custom_pipeline():
    """Example: Custom pipeline with custom transformations."""
    print("\n=== Custom Pipeline Example ===")
    
    # Create sample data
    import pandas as pd
    import numpy as np
    
    sample_data = pd.DataFrame({
        'user_id': range(1, 501),
        'age': np.random.randint(18, 80, 500),
        'income': np.random.normal(50000, 20000, 500),
        'credit_score': np.random.normal(700, 100, 500),
        'loan_amount': np.random.normal(100000, 50000, 500),
        'default_flag': np.random.choice([0, 1], 500, p=[0.9, 0.1])
    })
    
    input_path = "data/raw/sample_loans.csv"
    output_path = "data/processed/sample_loans_processed.parquet"
    
    # Ensure directory exists
    Path(input_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    sample_data.to_csv(input_path, index=False)
    print(f"Created sample data file: {input_path}")
    
    try:
        # Create custom pipeline with custom transformations
        pipeline = create_data_pipeline(
            source_type=DataSourceType.CSV,
            source_config={"file_path": input_path},
            output_path=output_path,
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=True
        )
        
        # Add custom transformation
        def add_risk_score(df):
            """Add risk score based on age, income, and credit score."""
            df['risk_score'] = (
                (df['age'] / 100) * 0.3 +
                (df['income'] / 100000) * 0.4 +
                (df['credit_score'] / 1000) * 0.3
            )
            return df
        
        pipeline.transformer.add_transformation(add_risk_score)
        
        # Run the pipeline
        results = pipeline.run_pipeline({"file_path": input_path})
        
        print(f"Custom pipeline completed successfully!")
        print(f"Results: {results}")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")


def example_api_pipeline():
    """Example: Pipeline with API data source."""
    print("\n=== API Pipeline Example ===")
    
    # Note: This is a mock example since we don't have a real API
    # In practice, you would use a real API endpoint
    
    try:
        # Create pipeline for API data
        pipeline = create_data_pipeline(
            source_type=DataSourceType.API,
            source_config={
                "api_url": "https://api.example.com/data",
                "params": {"limit": 1000}
            },
            output_path="data/processed/api_data.parquet",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=True
        )
        
        print("API pipeline configured (mock example)")
        print("In practice, this would connect to a real API endpoint")
        
    except Exception as e:
        print(f"Pipeline configuration failed: {str(e)}")


def example_streaming_pipeline():
    """Example: Streaming pipeline configuration."""
    print("\n=== Streaming Pipeline Example ===")
    
    try:
        # Create streaming pipeline
        config = DataPipelineConfig(
            source_type=DataSourceType.STREAM,
            source_config={"stream_config": "kafka://localhost:9092/topic"},
            processing_mode=ProcessingMode.STREAMING,
            batch_size=1000,
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=True,
            output_path="data/streaming/stream_output.parquet"
        )
        
        pipeline = DataPipeline(config)
        
        print("Streaming pipeline configured")
        print("In practice, this would process real-time data streams")
        
    except Exception as e:
        print(f"Pipeline configuration failed: {str(e)}")


def example_with_schema_validation():
    """Example: Pipeline with schema validation."""
    print("\n=== Schema Validation Example ===")
    
    # Create sample data
    import pandas as pd
    import numpy as np
    
    sample_data = pd.DataFrame({
        'customer_id': range(1, 101),
        'amount': np.random.normal(100, 50, 100),
        'transaction_date': pd.date_range('2024-01-01', periods=100, freq='H'),
        'category': np.random.choice(['food', 'transport', 'entertainment'], 100)
    })
    
    input_path = "data/raw/schema_test.csv"
    output_path = "data/processed/schema_test_processed.parquet"
    
    # Ensure directory exists
    Path(input_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    sample_data.to_csv(input_path, index=False)
    print(f"Created sample data file: {input_path}")
    
    # Define expected schema
    expected_schema = {
        'customer_id': int,
        'amount': float,
        'transaction_date': str,  # Will be converted to datetime
        'category': str
    }
    
    try:
        # Create pipeline with schema validation
        pipeline = create_data_pipeline(
            source_type=DataSourceType.CSV,
            source_config={"file_path": input_path},
            output_path=output_path,
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=True
        )
        
        # Run pipeline with schema validation
        results = pipeline.run_pipeline(
            source_config={"file_path": input_path},
            schema=expected_schema
        )
        
        print(f"Schema validation pipeline completed successfully!")
        print(f"Results: {results}")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")


def main():
    """Run all examples."""
    print("Data Pipeline Examples")
    print("=" * 50)
    
    # Create necessary directories
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(parents=True, exist_ok=True)
    
    # Run examples
    example_file_pipeline()
    example_custom_pipeline()
    example_api_pipeline()
    example_streaming_pipeline()
    example_with_schema_validation()
    
    # Note: Database example requires actual database connection
    # Uncomment the following line if you have a database set up
    # example_database_pipeline()
    
    print("\n" + "=" * 50)
    print("All examples completed!")
    print("Check the 'data/processed/' directory for output files.")


if __name__ == "__main__":
    main()