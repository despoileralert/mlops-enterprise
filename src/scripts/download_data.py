#!/usr/bin/env python3
"""
Data download script - entry point for data ingestion
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.pipeline.data_pipeline import DataIngestionPipeline
from src.logger import logging

def main():
    parser = argparse.ArgumentParser(description="Download data from MySQL database")
    parser.add_argument("--config", "-c", default="src/config/data_ingestion.yaml", 
                       help="Configuration file path")
    parser.add_argument("--tables", "-t", nargs="+", 
                       help="Specific tables to extract (default: all tables)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging level
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    try:
        # Initialize and run pipeline
        pipeline = DataIngestionPipeline(args.config)
        results = pipeline.run(args.tables)
        
        print(f"Successfully extracted {len(results)} files:")
        for file_path in results:
            print(f"  - {file_path}")
            
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()