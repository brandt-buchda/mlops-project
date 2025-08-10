import os
import yaml
import pandas as pd
import psycopg3
from google.cloud import bigquery
from google.cloud import storage
import logging
from typing import Dict, List
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseToGCPExtractor:
    def __init__(self, config_path: str = "config.yml"):
        """Initialize the extractor with configuration"""
        self.config = self._load_config(config_path)
        self.bq_client = bigquery.Client(project=self.config['project_id'])
        self.storage_client = storage.Client(project=self.config['project_id'])

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise

    def extract_from_postgres(self) -> pd.DataFrame:
        """Extract data from PostgreSQL database"""
        db_config = self.config['database']
        extraction_config = self.config['extraction']

        # Build the SQL query
        columns_str = ", ".join(extraction_config['columns'])
        query = f"SELECT {columns_str} FROM {extraction_config['table']}"

        try:
            logger.info("Connecting to PostgreSQL database...")

            # Get database credentials from environment variables or config
            conn_params = {
                'host': os.getenv('POSTGRES_HOST', db_config['host']),
                'database': os.getenv('POSTGRES_DB', db_config['name']),
                'user': os.getenv('POSTGRES_USER', db_config['user']),
                'password': os.getenv('POSTGRES_PASSWORD', db_config['password']),
                'port': int(os.getenv('POSTGRES_PORT', db_config['port']))
            }

            conn = psycopg2.connect(**conn_params)

            logger.info(f"Executing query: {query}")
            df = pd.read_sql_query(query, conn)

            logger.info(f"Successfully extracted {len(df)} rows from {extraction_config['table']}")
            conn.close()

            return df

        except Exception as e:
            logger.error(f"Error extracting data from PostgreSQL: {e}")
            raise

    def upload_to_bigquery(self, df: pd.DataFrame) -> None:
        """Upload DataFrame to BigQuery"""
        try:
            dataset_id = self.config['bq_dataset']
            table_id = self.config['upload']['bigquery_table']

            # Create dataset if it doesn't exist
            dataset_ref = self.bq_client.dataset(dataset_id)
            try:
                self.bq_client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_id} already exists")
            except:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.config.get('region', 'US').upper()
                self.bq_client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_id}")

            # Define table reference
            table_ref = dataset_ref.table(table_id)

            # Configure job to replace existing data
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.autodetect = True

            logger.info(f"Uploading {len(df)} rows to BigQuery table {dataset_id}.{table_id}...")

            # Upload data
            job = self.bq_client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()  # Wait for job to complete

            logger.info("Successfully uploaded data to BigQuery")

        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            raise

    def upload_to_gcs(self, df: pd.DataFrame) -> None:
        """Upload DataFrame to Google Cloud Storage"""
        try:
            # Extract bucket name from bucket_uri
            bucket_name = self.config['bucket_uri'].replace('gs://', '')
            filename = self.config['upload']['gcs_filename']

            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(f"data/{filename}")

            logger.info(f"Uploading {len(df)} rows to GCS: {self.config['bucket_uri']}/data/{filename}")

            # Upload as CSV
            csv_data = df.to_csv(index=False)
            blob.upload_from_string(csv_data, content_type='text/csv')

            logger.info("Successfully uploaded data to Cloud Storage")

        except Exception as e:
            logger.error(f"Error uploading to Cloud Storage: {e}")
            raise

    def run_extraction(self) -> None:
        """Run the complete extraction and upload process"""
        try:
            # Extract data from PostgreSQL
            df = self.extract_from_postgres()

            # Display extracted data info
            print(f"\n{'=' * 50}")
            print(f"Data extraction completed successfully!")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nFirst few rows:")
            print(df.head())
            print(f"{'=' * 50}\n")

            # Upload based on configuration
            upload_config = self.config['upload']

            if upload_config.get('to_bigquery', False):
                self.upload_to_bigquery(df)

            if upload_config.get('to_gcs', False):
                self.upload_to_gcs(df)

            logger.info("Data extraction and upload process completed successfully!")

        except Exception as e:
            logger.error(f"Error in extraction process: {e}")
            raise


def main():
    """Main execution function"""
    try:
        extractor = DatabaseToGCPExtractor()
        extractor.run_extraction()

    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()