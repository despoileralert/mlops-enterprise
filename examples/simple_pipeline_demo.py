#!/usr/bin/env python3
"""
Simple Data Pipeline Demonstration

This script demonstrates the data pipeline functionality using the simplified
version that doesn't require external dependencies.
"""

import sys
import json
import csv
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.data_pipeline_simple import (
    DataPipeline, 
    DataPipelineConfig, 
    DataSourceType, 
    ProcessingMode,
    create_data_pipeline,
    run_database_pipeline,
    run_file_pipeline
)


def create_sample_data():
    """Create sample data files for demonstration."""
    print("Creating sample data files...")
    
    # Create directories
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    
    # Create sample CSV file
    csv_data = [
        {"id": 1, "name": "Alice", "amount": 100.0, "category": "A"},
        {"id": 2, "name": "Bob", "amount": 200.0, "category": "B"},
        {"id": 3, "name": "Charlie", "amount": 300.0, "category": "A"},
        {"id": 4, "name": "Diana", "amount": 150.0, "category": "C"},
        {"id": 5, "name": "Eve", "amount": 250.0, "category": "B"}
    ]
    
    csv_file = "data/raw/sample_data.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "amount", "category"])
        writer.writeheader()
        writer.writerows(csv_data)
    
    print(f"Created CSV file: {csv_file}")
    
    # Create sample JSON file
    json_data = [
        {"user_id": 1, "score": 85, "status": "active", "department": "IT"},
        {"user_id": 2, "score": 92, "status": "active", "department": "HR"},
        {"user_id": 3, "score": 78, "status": "inactive", "department": "IT"},
        {"user_id": 4, "score": 95, "status": "active", "department": "Sales"},
        {"user_id": 5, "score": 88, "status": "active", "department": "Marketing"}
    ]
    
    json_file = "data/raw/sample_data.json"
    with open(json_file, 'w') as f:
        json.dump(json_data, f, indent=2)
    
    print(f"Created JSON file: {json_file}")
    
    return csv_file, json_file


def demo_database_pipeline():
    """Demonstrate database pipeline (mock)."""
    print("\n=== Database Pipeline Demo ===")
    
    try:
        results = run_database_pipeline(
            query="SELECT * FROM users LIMIT 10",
            output_path="data/processed/database_output.json",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        print("✅ Database pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ Database pipeline failed: {str(e)}")


def demo_file_pipeline():
    """Demonstrate file processing pipeline."""
    print("\n=== File Pipeline Demo ===")
    
    csv_file, json_file = create_sample_data()
    
    # Process CSV file
    try:
        results = run_file_pipeline(
            input_path=csv_file,
            output_path="data/processed/csv_output.json",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        print("✅ CSV pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ CSV pipeline failed: {str(e)}")
    
    # Process JSON file
    try:
        results = run_file_pipeline(
            input_path=json_file,
            output_path="data/processed/json_output.json",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        print("✅ JSON pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ JSON pipeline failed: {str(e)}")


def demo_custom_pipeline():
    """Demonstrate custom pipeline with transformations."""
    print("\n=== Custom Pipeline Demo ===")
    
    csv_file, _ = create_sample_data()
    
    try:
        # Create custom pipeline
        pipeline = create_data_pipeline(
            source_type=DataSourceType.CSV,
            source_config={"file_path": csv_file},
            output_path="data/processed/custom_output.json",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        # Add custom transformation
        def add_processed_flag(df):
            """Add a processed flag to each record."""
            for row in df.data:
                row['processed'] = True
                row['processed_timestamp'] = '2024-01-01T00:00:00Z'
            return df
        
        pipeline.transformer.add_transformation(add_processed_flag)
        
        # Run pipeline
        results = pipeline.run_pipeline({"file_path": csv_file})
        
        print("✅ Custom pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
        # Show output file content
        output_file = "data/processed/custom_output.json"
        if Path(output_file).exists():
            with open(output_file, 'r') as f:
                output_data = json.load(f)
            print(f"\nOutput data preview:")
            print(json.dumps(output_data[:2], indent=2))
        
    except Exception as e:
        print(f"❌ Custom pipeline failed: {str(e)}")


def demo_schema_validation():
    """Demonstrate schema validation."""
    print("\n=== Schema Validation Demo ===")
    
    csv_file, _ = create_sample_data()
    
    try:
        # Define expected schema
        schema = {
            'id': int,
            'name': str,
            'amount': float,
            'category': str
        }
        
        # Create pipeline with schema validation
        pipeline = create_data_pipeline(
            source_type=DataSourceType.CSV,
            source_config={"file_path": csv_file},
            output_path="data/processed/schema_validated.json",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        # Run pipeline with schema validation
        results = pipeline.run_pipeline(
            {"file_path": csv_file},
            schema=schema
        )
        
        print("✅ Schema validation pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ Schema validation pipeline failed: {str(e)}")


def demo_error_handling():
    """Demonstrate error handling."""
    print("\n=== Error Handling Demo ===")
    
    try:
        # Try to process non-existent file
        results = run_file_pipeline(
            input_path="data/raw/non_existent_file.csv",
            output_path="data/processed/error_output.json",
            validation_enabled=True,
            transformation_enabled=True,
            monitoring_enabled=False
        )
        
        print("❌ Expected error was not raised")
        
    except Exception as e:
        print("✅ Error handling worked correctly!")
        print(f"Error: {str(e)}")


def demo_configuration():
    """Demonstrate different configuration options."""
    print("\n=== Configuration Demo ===")
    
    csv_file, _ = create_sample_data()
    
    # Different configuration scenarios
    configs = [
        {
            "name": "Basic Pipeline",
            "config": {
                "validation_enabled": True,
                "transformation_enabled": True,
                "monitoring_enabled": False
            }
        },
        {
            "name": "Validation Only",
            "config": {
                "validation_enabled": True,
                "transformation_enabled": False,
                "monitoring_enabled": False
            }
        },
        {
            "name": "Transformation Only",
            "config": {
                "validation_enabled": False,
                "transformation_enabled": True,
                "monitoring_enabled": False
            }
        }
    ]
    
    for i, config_info in enumerate(configs):
        print(f"\n--- {config_info['name']} ---")
        
        try:
            results = run_file_pipeline(
                input_path=csv_file,
                output_path=f"data/processed/config_{i}_output.json",
                **config_info['config']
            )
            
            print(f"✅ {config_info['name']} completed successfully!")
            print(f"Duration: {results.get('pipeline_duration', 0):.3f}s")
            print(f"Rows processed: {results.get('metrics', {}).get('ingestion_rows', 0)}")
            
        except Exception as e:
            print(f"❌ {config_info['name']} failed: {str(e)}")


def main():
    """Run all demonstrations."""
    print("Data Pipeline Demonstration")
    print("=" * 50)
    
    # Create necessary directories
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(parents=True, exist_ok=True)
    
    # Run demonstrations
    demo_database_pipeline()
    demo_file_pipeline()
    demo_custom_pipeline()
    demo_schema_validation()
    demo_error_handling()
    demo_configuration()
    
    print("\n" + "=" * 50)
    print("All demonstrations completed!")
    print("\nGenerated files:")
    
    # List generated files
    processed_dir = Path("data/processed")
    if processed_dir.exists():
        for file in processed_dir.glob("*.json"):
            print(f"  - {file}")
    
    print("\nYou can examine the generated files to see the pipeline outputs.")


if __name__ == "__main__":
    main()