#!/usr/bin/env python3
"""
Standalone Data Pipeline Demonstration

This script demonstrates the core concepts of a data pipeline
without any external dependencies.
"""

import json
import csv
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum


# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataSourceType(str, Enum):
    """Supported data source types."""
    CSV = "csv"
    JSON = "json"
    DATABASE = "database"
    API = "api"


@dataclass
class PipelineConfig:
    """Simple pipeline configuration."""
    source_type: DataSourceType
    source_path: str
    output_path: str
    validation_enabled: bool = True
    transformation_enabled: bool = True
    batch_size: int = 1000


class SimpleDataFrame:
    """Simple data structure to represent tabular data."""
    
    def __init__(self, data: List[Dict[str, Any]]):
        self.data = data
        self.columns = list(data[0].keys()) if data else []
    
    def __len__(self):
        return len(self.data)
    
    def copy(self):
        return SimpleDataFrame(self.data.copy())
    
    def to_json(self, file_path: str):
        with open(file_path, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def to_csv(self, file_path: str):
        if not self.data:
            return
        
        with open(file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(self.data)


class DataValidator:
    """Data validation component."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
    
    def validate_schema(self, data: SimpleDataFrame, expected_schema: Dict[str, type]) -> bool:
        """Validate data against expected schema."""
        if not data.data:
            logger.error("Empty dataset")
            return False
        
        for column, expected_type in expected_schema.items():
            if column not in data.columns:
                logger.error(f"Missing required column: {column}")
                return False
            
            # Check data types for first few rows
            for row in data.data[:5]:
                if column in row and not isinstance(row[column], expected_type):
                    logger.error(f"Column {column} has incorrect data type")
                    return False
        
        logger.info("Schema validation passed")
        return True
    
    def check_quality(self, data: SimpleDataFrame) -> Dict[str, Any]:
        """Perform data quality checks."""
        quality_metrics = {
            "total_rows": len(data.data),
            "null_counts": {},
            "duplicate_rows": 0
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
        
        logger.info(f"Quality metrics: {quality_metrics}")
        return quality_metrics


class DataTransformer:
    """Data transformation component."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.transformations: List[Callable] = []
    
    def add_transformation(self, transformation_func: Callable):
        """Add a custom transformation."""
        self.transformations.append(transformation_func)
        logger.info(f"Added transformation: {transformation_func.__name__}")
    
    def apply_transformations(self, data: SimpleDataFrame) -> SimpleDataFrame:
        """Apply all registered transformations."""
        if not self.transformations:
            logger.warning("No transformations registered")
            return data
        
        transformed_data = data.copy()
        
        for i, transformation in enumerate(self.transformations):
            try:
                start_time = time.time()
                transformed_data = transformation(transformed_data)
                duration = time.time() - start_time
                logger.info(f"Applied transformation {i+1}/{len(self.transformations)} in {duration:.2f}s")
            except Exception as e:
                logger.error(f"Transformation failed: {str(e)}")
                raise
        
        return transformed_data
    
    def standard_transformations(self, data: SimpleDataFrame) -> SimpleDataFrame:
        """Apply standard data cleaning."""
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
                        row[key] = 0
                    else:
                        row[key] = "Unknown"
        
        return SimpleDataFrame(unique_data)


class DataIngestor:
    """Data ingestion component."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
    
    def ingest_from_file(self, file_path: str) -> SimpleDataFrame:
        """Ingest data from file."""
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
        
        logger.info(f"Ingested {len(data)} rows from {file_path}")
        return SimpleDataFrame(data)
    
    def ingest_from_database(self, query: str) -> SimpleDataFrame:
        """Mock database ingestion."""
        # Simulate database data
        mock_data = [
            {"id": 1, "name": "Alice", "amount": 100.0},
            {"id": 2, "name": "Bob", "amount": 200.0},
            {"id": 3, "name": "Charlie", "amount": 300.0}
        ]
        
        logger.info(f"Ingested {len(mock_data)} rows from database (mock)")
        return SimpleDataFrame(mock_data)


class DataPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.validator = DataValidator(config)
        self.transformer = DataTransformer(config)
        self.ingestor = DataIngestor(config)
    
    def run(self, schema: Optional[Dict[str, type]] = None) -> Dict[str, Any]:
        """Execute the complete pipeline."""
        start_time = time.time()
        results = {
            "pipeline_start": datetime.now().isoformat(),
            "config": self.config.__dict__,
            "metrics": {}
        }
        
        try:
            # Step 1: Data Ingestion
            logger.info("Starting data ingestion...")
            if self.config.source_type == DataSourceType.CSV:
                data = self.ingestor.ingest_from_file(self.config.source_path)
            elif self.config.source_type == DataSourceType.JSON:
                data = self.ingestor.ingest_from_file(self.config.source_path)
            elif self.config.source_type == DataSourceType.DATABASE:
                data = self.ingestor.ingest_from_database("SELECT * FROM table")
            else:
                raise ValueError(f"Unsupported source type: {self.config.source_type}")
            
            results["metrics"]["ingestion_rows"] = len(data)
            
            # Step 2: Data Validation
            if self.config.validation_enabled:
                logger.info("Starting data validation...")
                if schema:
                    results["validation"] = {
                        "schema_valid": self.validator.validate_schema(data, schema)
                    }
                results["validation"]["quality_metrics"] = self.validator.check_quality(data)
            
            # Step 3: Data Transformation
            if self.config.transformation_enabled:
                logger.info("Starting data transformation...")
                data = self.transformer.apply_transformations(data)
                data = self.transformer.standard_transformations(data)
                results["metrics"]["transformed_rows"] = len(data)
            
            # Step 4: Data Storage
            logger.info("Starting data storage...")
            output_path = Path(self.config.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if output_path.suffix.lower() == '.csv':
                data.to_csv(output_path)
            else:
                data.to_json(output_path)
            
            results["storage"] = {"output_path": str(output_path)}
            
            # Step 5: Finalize results
            duration = time.time() - start_time
            results["pipeline_duration"] = duration
            results["status"] = "success"
            
            logger.info(f"Pipeline completed successfully in {duration:.2f}s")
            
        except Exception as e:
            duration = time.time() - start_time
            results["pipeline_duration"] = duration
            results["status"] = "failed"
            results["error"] = str(e)
            
            logger.error(f"Pipeline failed after {duration:.2f}s: {str(e)}")
            raise
        
        return results


def create_sample_data():
    """Create sample data files."""
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


def demo_basic_pipeline():
    """Demonstrate basic pipeline functionality."""
    print("\n=== Basic Pipeline Demo ===")
    
    csv_file, _ = create_sample_data()
    
    config = PipelineConfig(
        source_type=DataSourceType.CSV,
        source_path=csv_file,
        output_path="data/processed/basic_output.json",
        validation_enabled=True,
        transformation_enabled=True
    )
    
    pipeline = DataPipeline(config)
    
    try:
        results = pipeline.run()
        print("✅ Basic pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ Basic pipeline failed: {str(e)}")


def demo_schema_validation():
    """Demonstrate schema validation."""
    print("\n=== Schema Validation Demo ===")
    
    csv_file, _ = create_sample_data()
    
    config = PipelineConfig(
        source_type=DataSourceType.CSV,
        source_path=csv_file,
        output_path="data/processed/schema_output.json",
        validation_enabled=True,
        transformation_enabled=True
    )
    
    pipeline = DataPipeline(config)
    
    # Define expected schema
    schema = {
        'id': int,
        'name': str,
        'amount': float,
        'category': str
    }
    
    try:
        results = pipeline.run(schema=schema)
        print("✅ Schema validation pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ Schema validation pipeline failed: {str(e)}")


def demo_custom_transformations():
    """Demonstrate custom transformations."""
    print("\n=== Custom Transformations Demo ===")
    
    csv_file, _ = create_sample_data()
    
    config = PipelineConfig(
        source_type=DataSourceType.CSV,
        source_path=csv_file,
        output_path="data/processed/transformed_output.json",
        validation_enabled=True,
        transformation_enabled=True
    )
    
    pipeline = DataPipeline(config)
    
    # Add custom transformation
    def add_processed_flag(data):
        """Add processing metadata to each record."""
        for row in data.data:
            row['processed'] = True
            row['processed_timestamp'] = '2024-01-01T00:00:00Z'
        return data
    
    pipeline.transformer.add_transformation(add_processed_flag)
    
    try:
        results = pipeline.run()
        print("✅ Custom transformations pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
        # Show output data
        output_file = "data/processed/transformed_output.json"
        if Path(output_file).exists():
            with open(output_file, 'r') as f:
                output_data = json.load(f)
            print(f"\nOutput data preview:")
            print(json.dumps(output_data[:2], indent=2))
        
    except Exception as e:
        print(f"❌ Custom transformations pipeline failed: {str(e)}")


def demo_json_pipeline():
    """Demonstrate JSON file processing."""
    print("\n=== JSON Pipeline Demo ===")
    
    _, json_file = create_sample_data()
    
    config = PipelineConfig(
        source_type=DataSourceType.JSON,
        source_path=json_file,
        output_path="data/processed/json_output.json",
        validation_enabled=True,
        transformation_enabled=True
    )
    
    pipeline = DataPipeline(config)
    
    try:
        results = pipeline.run()
        print("✅ JSON pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ JSON pipeline failed: {str(e)}")


def demo_error_handling():
    """Demonstrate error handling."""
    print("\n=== Error Handling Demo ===")
    
    config = PipelineConfig(
        source_type=DataSourceType.CSV,
        source_path="data/raw/non_existent_file.csv",
        output_path="data/processed/error_output.json",
        validation_enabled=True,
        transformation_enabled=True
    )
    
    pipeline = DataPipeline(config)
    
    try:
        results = pipeline.run()
        print("❌ Expected error was not raised")
        
    except Exception as e:
        print("✅ Error handling worked correctly!")
        print(f"Error: {str(e)}")


def demo_database_pipeline():
    """Demonstrate database pipeline (mock)."""
    print("\n=== Database Pipeline Demo ===")
    
    config = PipelineConfig(
        source_type=DataSourceType.DATABASE,
        source_path="",  # Not used for database
        output_path="data/processed/database_output.json",
        validation_enabled=True,
        transformation_enabled=True
    )
    
    pipeline = DataPipeline(config)
    
    try:
        results = pipeline.run()
        print("✅ Database pipeline completed successfully!")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        print(f"❌ Database pipeline failed: {str(e)}")


def main():
    """Run all demonstrations."""
    print("Standalone Data Pipeline Demonstration")
    print("=" * 50)
    
    # Create necessary directories
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    
    # Run demonstrations
    demo_basic_pipeline()
    demo_schema_validation()
    demo_custom_transformations()
    demo_json_pipeline()
    demo_error_handling()
    demo_database_pipeline()
    
    print("\n" + "=" * 50)
    print("All demonstrations completed!")
    print("\nGenerated files:")
    
    # List generated files
    processed_dir = Path("data/processed")
    if processed_dir.exists():
        for file in processed_dir.glob("*.json"):
            print(f"  - {file}")
    
    print("\nYou can examine the generated files to see the pipeline outputs.")
    print("\nThis demonstration shows the core concepts of a data pipeline:")
    print("- Data ingestion from multiple sources")
    print("- Data validation and quality checks")
    print("- Data transformation and cleaning")
    print("- Error handling and logging")
    print("- Configurable pipeline components")


if __name__ == "__main__":
    main()