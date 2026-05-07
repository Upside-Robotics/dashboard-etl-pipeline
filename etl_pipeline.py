"""
Main ETL Pipeline Script
Orchestrates data extraction from PostgreSQL and prepares for warehouse loading
"""

import logging
import json
import sys
from pathlib import Path
from datetime import datetime
from postgres_connector import PostgreSQLConnector
from config import ETL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create output directory for extracted data
OUTPUT_DIR = Path("./extracted_data")
OUTPUT_DIR.mkdir(exist_ok=True)


class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self):
        self.connector = PostgreSQLConnector()
        self.extraction_stats = {
            'start_time': None,
            'end_time': None,
            'total_rows_extracted': 0,
            'batches_processed': 0,
            'status': 'pending'
        }
    
    def extract_from_postgres(self, table_name: str, output_format: str = 'jsonl') -> bool:
        """
        Extract data from PostgreSQL table
        
        Args:
            table_name: Name of source table
            output_format: Output format ('jsonl' or 'csv')
            
        Returns:
            bool: Success status
        """
        logger.info(f"Starting extraction from table '{table_name}'")
        self.extraction_stats['start_time'] = datetime.now()
        
        if not self.connector.connect():
            logger.error("Failed to connect to database")
            self.extraction_stats['status'] = 'failed'
            return False
        
        try:
            # Get table info
            table_info = self.connector.get_table_info(table_name)
            if not table_info:
                logger.error(f"Table '{table_name}' not found or inaccessible")
                self.extraction_stats['status'] = 'failed'
                return False
            
            # Create output file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = OUTPUT_DIR / f"{table_name}_{timestamp}.{output_format}"
            
            logger.info(f"Extracting {table_info['row_count']} rows from '{table_name}'")
            logger.info(f"Output file: {output_file}")
            
            if output_format == 'jsonl':
                self._extract_to_jsonl(table_name, output_file, table_info)
            elif output_format == 'csv':
                self._extract_to_csv(table_name, output_file, table_info)
            else:
                logger.error(f"Unsupported output format: {output_format}")
                self.extraction_stats['status'] = 'failed'
                return False
            
            self.extraction_stats['end_time'] = datetime.now()
            self.extraction_stats['status'] = 'success'
            
            duration = (self.extraction_stats['end_time'] - 
                       self.extraction_stats['start_time']).total_seconds()
            logger.info(f"Extraction completed in {duration:.2f} seconds")
            logger.info(f"Total rows extracted: {self.extraction_stats['total_rows_extracted']}")
            logger.info(f"Batches processed: {self.extraction_stats['batches_processed']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            self.extraction_stats['status'] = 'failed'
            return False
        
        finally:
            self.connector.disconnect()
    
    def _extract_to_jsonl(self, table_name: str, output_file: Path, table_info: dict):
        """Extract data to JSONL format (one JSON object per line)"""
        try:
            with open(output_file, 'w') as f:
                for batch in self.connector.retrieve_data_batched(table_name):
                    for row in batch:
                        # Convert datetime objects to ISO format strings
                        row_serialized = self._serialize_row(row)
                        f.write(json.dumps(row_serialized) + '\n')
                        self.extraction_stats['total_rows_extracted'] += 1
                    
                    self.extraction_stats['batches_processed'] += 1
            
            logger.info(f"Data saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error writing to JSONL file: {e}")
            raise
    
    def _extract_to_csv(self, table_name: str, output_file: Path, table_info: dict):
        """Extract data to CSV format"""
        import csv
        
        try:
            column_names = [col['name'] for col in table_info['columns']]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=column_names)
                writer.writeheader()
                
                for batch in self.connector.retrieve_data_batched(table_name):
                    for row in batch:
                        row_serialized = self._serialize_row(row)
                        writer.writerow(row_serialized)
                        self.extraction_stats['total_rows_extracted'] += 1
                    
                    self.extraction_stats['batches_processed'] += 1
            
            logger.info(f"Data saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error writing to CSV file: {e}")
            raise
    
    def _serialize_row(self, row: dict) -> dict:
        """Convert row data to JSON-serializable format"""
        serialized = {}
        for key, value in row.items():
            if hasattr(value, 'isoformat'):  # datetime objects
                serialized[key] = value.isoformat()
            elif isinstance(value, (bytes, bytearray)):
                serialized[key] = value.hex()
            else:
                serialized[key] = value
        return serialized
    
    def get_extraction_stats(self) -> dict:
        """Get extraction statistics"""
        return self.extraction_stats


def main():
    """Main execution"""
    logger.info("=" * 60)
    logger.info("ETL Pipeline Started")
    logger.info("=" * 60)
    
    pipeline = ETLPipeline()
    
    # Extract data from robot_executive_state table
    success = pipeline.extract_from_postgres(
        table_name=ETL_CONFIG['source_table'],
        output_format='jsonl'  # Can also use 'csv'
    )
    
    # Print statistics
    stats = pipeline.get_extraction_stats()
    print("\n" + "=" * 60)
    print("EXTRACTION STATISTICS")
    print("=" * 60)
    print(f"Status: {stats['status']}")
    print(f"Total Rows: {stats['total_rows_extracted']}")
    print(f"Batches Processed: {stats['batches_processed']}")
    if stats['start_time'] and stats['end_time']:
        duration = (stats['end_time'] - stats['start_time']).total_seconds()
        print(f"Duration: {duration:.2f} seconds")
        if stats['total_rows_extracted'] > 0:
            print(f"Throughput: {stats['total_rows_extracted'] / duration:.2f} rows/sec")
    print("=" * 60 + "\n")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
