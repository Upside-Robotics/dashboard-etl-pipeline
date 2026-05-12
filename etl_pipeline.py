"""
Main ETL Pipeline Script
Orchestrates data extraction from PostgreSQL and loads data to S3 and Amazon Redshift
"""

import csv
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from postgres_connector import PostgreSQLConnector
from warehouse_loader import S3Uploader, RedshiftConnector
from config import ETL_CONFIG, S3_CONFIG, AWS_CONFIG, REDSHIFT_CONFIG

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
        self.s3_uploader = S3Uploader(
            bucket_name=S3_CONFIG['bucket_name'],
            region_name=S3_CONFIG['region_name'],
            prefix=S3_CONFIG.get('prefix'),
            aws_config=AWS_CONFIG,
        )
        self.redshift_loader = RedshiftConnector(REDSHIFT_CONFIG)
        self.extraction_stats = {
            'start_time': None,
            'end_time': None,
            'total_rows_extracted': 0,
            'batches_processed': 0,
            'status': 'pending',
            's3_uri': None,
            'redshift_status': 'pending',
            'local_csv_path': None,
        }

    def _serialize_row(self, row: dict) -> dict:
        """Convert row data to JSON-serializable format"""
        serialized = {}
        for key, value in row.items():
            if hasattr(value, 'isoformat'):
                serialized[key] = value.isoformat()
            elif isinstance(value, (bytes, bytearray)):
                serialized[key] = value.hex()
            else:
                serialized[key] = value
        return serialized

    def _extract_to_csv_file(self, table_name: str, output_file: Path, table_info: dict):
        """Extract table data into a CSV file"""
        column_names = [col['name'] for col in table_info['columns']]

        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=column_names,
                    extrasaction='ignore',
                    quoting=csv.QUOTE_MINIMAL,
                )
                writer.writeheader()

                for batch in self.connector.retrieve_data_batched(table_name):
                    for row in batch:
                        row_serialized = self._serialize_row(row)
                        writer.writerow(row_serialized)
                        self.extraction_stats['total_rows_extracted'] += 1

                    self.extraction_stats['batches_processed'] += 1

            logger.info(f"Data saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error writing to CSV file: {e}")
            raise

    def extract_and_stage_to_s3(self, table_name: str) -> str:
        """Extract data from PostgreSQL and upload the CSV to S3"""
        logger.info(f"Starting extraction and staging to S3 for '{table_name}'")
        self.extraction_stats['start_time'] = datetime.now()

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            if attempt > 1:
                logger.warning(f"Retrying extraction (attempt {attempt}/{max_retries}) in 10s...")
                time.sleep(10)
                self.extraction_stats['total_rows_extracted'] = 0
                self.extraction_stats['batches_processed'] = 0

            if not self.connector.connect():
                logger.error("Failed to connect to PostgreSQL")
                self.extraction_stats['status'] = 'failed'
                return ""

            try:
                table_info = self.connector.get_table_info(table_name)
                if not table_info:
                    logger.error(f"Table '{table_name}' not found or inaccessible")
                    self.extraction_stats['status'] = 'failed'
                    return ""

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = OUTPUT_DIR / f"{table_name}_{timestamp}.csv"
                self.extraction_stats['local_csv_path'] = str(output_file)

                logger.info(f"Extracting {table_info['row_count']} rows from '{table_name}'")
                local_path = self._extract_to_csv_file(table_name, output_file, table_info)

                if self.extraction_stats['total_rows_extracted'] == 0:
                    logger.error("Aborting load because extraction failed or produced 0 rows. Redshift table was not modified.")
                    self.extraction_stats['status'] = 'failed'
                    return ""

                s3_uri = self.s3_uploader.upload_file(local_path)
                self.extraction_stats['s3_uri'] = s3_uri
                self.extraction_stats['status'] = 'staged'

                logger.info(f"Uploaded extract to S3: {s3_uri}")
                return s3_uri

            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"Extraction attempt {attempt} failed: {e}")
                else:
                    logger.error(f"Extraction and staging failed after {max_retries} attempts: {e}")
                    self.extraction_stats['status'] = 'failed'
                    return ""

            finally:
                self.connector.disconnect()

        self.extraction_stats['status'] = 'failed'
        return ""

    def _csv_has_data_rows(self, csv_path: Path) -> bool:
        with open(csv_path, encoding='utf-8') as f:
            f.readline()  # skip header
            return bool(f.readline())  # True if at least one data row exists

    def load_from_s3_to_redshift(self, s3_uri: str) -> bool:
        """Load a staged S3 file into Redshift using COPY"""
        if not s3_uri:
            logger.error("No S3 URI provided for Redshift load")
            self.extraction_stats['redshift_status'] = 'failed'
            return False

        rows_extracted = self.extraction_stats['total_rows_extracted']
        if rows_extracted <= 0:
            logger.error("Aborting load because extraction failed or produced 0 rows. Redshift table was not modified.")
            self.extraction_stats['redshift_status'] = 'failed'
            return False

        local_csv = self.extraction_stats.get('local_csv_path')
        if not local_csv or not Path(local_csv).exists():
            logger.error("Aborting load because extraction failed or produced 0 rows. Redshift table was not modified.")
            self.extraction_stats['redshift_status'] = 'failed'
            return False

        if not self._csv_has_data_rows(Path(local_csv)):
            logger.error("Aborting load because extraction failed or produced 0 rows. Redshift table was not modified.")
            self.extraction_stats['redshift_status'] = 'failed'
            return False

        target_table = f"{REDSHIFT_CONFIG['schema']}.{REDSHIFT_CONFIG['table']}"
        copy_options = REDSHIFT_CONFIG.get('copy_options', {})

        if not self.redshift_loader.connect():
            logger.error("Failed to connect to Redshift")
            self.extraction_stats['redshift_status'] = 'failed'
            return False

        try:
            success = self.redshift_loader.copy_from_s3(
                target_table=target_table,
                s3_uri=s3_uri,
                region=S3_CONFIG['region_name'],
                iam_role_arn=copy_options.get('iam_role_arn'),
                aws_credentials={
                    'aws_access_key_id': AWS_CONFIG.get('aws_access_key_id'),
                    'aws_secret_access_key': AWS_CONFIG.get('aws_secret_access_key'),
                    'aws_session_token': AWS_CONFIG.get('aws_session_token'),
                },
                file_format=copy_options.get('file_format', 'csv'),
                delimiter=copy_options.get('delimiter', ','),
                ignore_header=copy_options.get('ignore_header', 1),
            )
            self.extraction_stats['redshift_status'] = 'success' if success else 'failed'
            return success

        except Exception as e:
            logger.error(f"Redshift load failed: {e}")
            self.extraction_stats['redshift_status'] = 'failed'
            return False

        finally:
            self.redshift_loader.disconnect()

    def _cleanup_old_s3_files(self):
        """Delete staging files from S3 that were uploaded before today"""
        today = datetime.now().strftime("%Y%m%d")
        bucket = S3_CONFIG['bucket_name']
        prefix = S3_CONFIG.get('prefix', '').rstrip('/') + '/'

        try:
            paginator = self.s3_uploader.client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if today not in key:
                        self.s3_uploader.client.delete_object(Bucket=bucket, Key=key)
                        logger.info(f"Deleted old S3 file: s3://{bucket}/{key}")
        except Exception as e:
            logger.warning(f"S3 cleanup failed (non-fatal): {e}")

    def run_full_redshift_load(self, table_name: str) -> bool:
        """Run the full ETL flow: extract from Postgres, stage to S3, and load to Redshift"""
        s3_uri = self.extract_and_stage_to_s3(table_name)
        if not s3_uri:
            return False

        success = self.load_from_s3_to_redshift(s3_uri)
        if success:
            self._cleanup_old_s3_files()
        return success

    def get_extraction_stats(self) -> dict:
        """Get extraction statistics"""
        return self.extraction_stats


def main():
    """Main execution"""
    logger.info("=" * 60)
    logger.info("ETL Pipeline Started")
    logger.info("=" * 60)

    pipeline = ETLPipeline()
    success = pipeline.run_full_redshift_load(ETL_CONFIG['source_table'])

    stats = pipeline.get_extraction_stats()
    print("\n" + "=" * 60)
    print("ETL STATISTICS")
    print("=" * 60)
    print(f"Status: {stats['status']}")
    print(f"S3 URI: {stats.get('s3_uri')}" )
    print(f"Redshift Load Status: {stats.get('redshift_status')}" )
    print(f"Total Rows Extracted: {stats['total_rows_extracted']}")
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
