import os
import yaml
import pandas as pd
import psycopg
from google.cloud import bigquery
import logging
from typing import Dict, List
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _load_config(config_path: str) -> Dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise


class DatabaseToGCPExtractor:
    def __init__(self, config_path: str = "../config.yml"):
        self.config = _load_config(config_path)
        self.bq_client = bigquery.Client(project=self.config['project_id'])

    def extract_from_postgres(self) -> pd.DataFrame:
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
                'dbname': os.getenv('POSTGRES_DB', db_config['name']),
                'user': os.getenv('POSTGRES_USER', db_config['user']),
                'password': os.getenv('POSTGRES_PASSWORD', db_config['password']),
                'port': int(os.getenv('POSTGRES_PORT', db_config['port']))
            }

            conn = psycopg.connect(**conn_params)

            logger.info(f"Executing query: {query}")
            df = pd.read_sql_query(query, conn)

            logger.info(f"Successfully extracted {len(df)} rows from {extraction_config['table']}")
            conn.close()

            return df

        except Exception as e:
            logger.error(f"Error extracting data from PostgreSQL: {e}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data to match BigQuery schema"""
        try:
            logger.info("Transforming data to match BigQuery schema...")

            # Map database columns to BigQuery columns
            column_mapping = {
                "name_raw": "name",
                "description_raw": "description",
                "variant_name_raw": "variant_name",
                "url": "url",
                "weight_lbs": "target",
            }

            transformed_df = df.rename(columns=column_mapping)

            # Ensure required columns exist; create if missing
            required_cols = ["name", "description", "variant_name", "url", "target"]
            for col in required_cols:
                if col not in transformed_df.columns:
                    transformed_df[col] = pd.NA

            # Keep only required columns, in order
            transformed_df = transformed_df[required_cols]

            # Coerce types safely
            for col in ["name", "description", "variant_name", "url"]:
                transformed_df[col] = transformed_df[col].astype("string")

            # Make target numeric (float64); invalid parses become NaN
            transformed_df["target"] = pd.to_numeric(
                transformed_df["target"], errors="coerce"
            ).astype("float64")

            logger.info("Data transformation completed. Final shape: %s", transformed_df.shape)
            logger.info("Columns: %s", list(transformed_df.columns))
            return transformed_df

        except Exception as e:
            logger.error("Error transforming data: %s", e)
            raise

    def upload_to_bigquery(self, df: pd.DataFrame) -> None:
        """Upload DataFrame to BigQuery"""
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound

        try:
            dataset_id = self.config["bq_dataset"]
            table_id = self.config["bq_table"]

            # Create dataset if it doesn't exist
            dataset_ref = self.bq_client.dataset(dataset_id)
            try:
                self.bq_client.get_dataset(dataset_ref)
                logger.info(f"Dataset {dataset_id} already exists")
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.config.get("region", "US").upper()
                self.bq_client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_id}")

            table_ref = dataset_ref.table(table_id)

            # Expected schema (target as FLOAT to match float64 in DataFrame)
            expected_schema = [
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("variant_name", "STRING"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("target", "FLOAT"),
            ]

            # Ensure table exists with expected schema; recreate if mismatched
            recreate_table = False
            try:
                table = self.bq_client.get_table(table_ref)
                existing = {f.name: f.field_type for f in table.schema}
                for f in expected_schema:
                    if existing.get(f.name) != f.field_type:
                        logger.info(
                            "Schema mismatch for field '%s': existing=%s expected=%s",
                            f.name, existing.get(f.name), f.field_type
                        )
                        recreate_table = True
                        break
            except NotFound:
                recreate_table = True

            if recreate_table:
                try:
                    self.bq_client.delete_table(table_ref, not_found_ok=True)
                    logger.info(f"Deleted existing table {dataset_id}.{table_id}")
                except Exception as e:
                    logger.warning("Could not delete table (may not exist): %s", e)

                table = bigquery.Table(table_ref, schema=expected_schema)
                self.bq_client.create_table(table)
                logger.info(f"Created table {dataset_id}.{table_id} with expected schema")

            # Coerce DataFrame types and replace NaN with None
            df_to_load = df.copy()
            for col in ["name", "description", "variant_name", "url"]:
                df_to_load[col] = df_to_load[col].astype("string")
            df_to_load["target"] = pd.to_numeric(df_to_load["target"], errors="coerce").astype("float64")
            df_to_load = df_to_load.where(pd.notnull(df_to_load), None)

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                schema=expected_schema,
            )

            logger.info(f"Uploading {len(df_to_load)} rows to BigQuery table {dataset_id}.{table_id}...")
            job = self.bq_client.load_table_from_dataframe(df_to_load, table_ref, job_config=job_config)
            job.result()
            logger.info("Upload to BigQuery completed successfully.")

        except Exception as e:
            logger.error("Error uploading to BigQuery: %s", e)
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
            raw_df = self.extract_from_postgres()

            # Transform data to match BigQuery schema
            df = self.transform_data(raw_df)

            # Display extracted data info
            print(f"\n{'=' * 60}")
            print(f"Data extraction and transformation completed successfully!")
            print(f"Original shape: {raw_df.shape}")
            print(f"Transformed shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nFirst few rows:")
            print(df.head())
            print(f"\nTarget column stats:")
            print(f"Non-null targets: {df['target'].notna().sum()}")
            print(f"Target range: {df['target'].min():.2f} - {df['target'].max():.2f} lbs")
            print(f"{'=' * 60}\n")

            self.upload_to_bigquery(df)

            logger.info("Data extraction and upload process completed successfully!")

        except Exception as e:
            logger.error(f"Error in extraction process: {e}")
            raise


def main():
    try:
        extractor = DatabaseToGCPExtractor()
        extractor.run_extraction()

    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()