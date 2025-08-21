import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import aiplatform
import yaml
import logging
import argparse
import os
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yml") -> Dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise


def clean_csv_data(csv_path: str) -> pd.DataFrame:
    """Clean and transform the weight extraction CSV data"""
    logger.info(f"Loading CSV data from {csv_path}")

    # Load the CSV
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from CSV")

    # Filter out rows with unwanted URLs
    initial_count = len(df)
    if 'url' in df.columns:
        df = df[~df['url'].str.contains('https://shopalderspring.com', na=False)]
        df = df[~df['url'].str.contains('https://cartercountrymeats.com', na=False)]
        logger.info(f"Filtered out {initial_count - len(df)} rows with excluded URLs")

    # Drop rows where extracted_weight or extracted_quantity are null
    initial_count = len(df)
    df = df.dropna(subset=['extracted_weight', 'extracted_quantity'])
    logger.info(f"Dropped {initial_count - len(df)} rows with null extracted_weight or extracted_quantity")

    # Drop timestamp column
    if 'timestamp' in df.columns:
        df = df.drop('timestamp', axis=1)

    # Rename columns
    column_mapping = {
        'name_raw': 'name',
        'variant_name_raw': 'variant',
        'description_raw': 'description',
        'weight_raw': 'weight',
        'final_result': 'target'
    }
    df = df.rename(columns=column_mapping)

    # Create target_text column in the format "quantity x weight lbs"
    df['target_text'] = df.apply(lambda row: f"{int(row['extracted_quantity'])} x {row['extracted_weight']:.1f} lbs",
                                 axis=1)

    # Ensure all string columns are properly typed (including url)
    string_columns = ['name', 'variant', 'description', 'weight', 'extraction_method', 'url']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype('string')

    # Ensure numeric columns are properly typed
    numeric_columns = ['extracted_weight', 'extracted_quantity', 'target']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Handle any remaining null values in target
    df = df.dropna(subset=['target'])

    logger.info(f"Cleaned data shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")

    return df


def upload_to_bigquery(df: pd.DataFrame, config: Dict, table_suffix: str = "_training") -> str:
    """Upload cleaned DataFrame to BigQuery"""
    client = bigquery.Client(project=config['project_id'])

    dataset_id = config['bq_dataset']
    table_id = config['bq_table'] + table_suffix

    # Create the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define schema for the training table (including url column)
    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("variant", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("weight", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("extracted_weight", "FLOAT"),
        bigquery.SchemaField("extracted_quantity", "INTEGER"),
        bigquery.SchemaField("extraction_method", "STRING"),
        bigquery.SchemaField("target", "FLOAT"),
        bigquery.SchemaField("target_text", "STRING"),
    ]

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    try:
        logger.info(f"Uploading {len(df)} rows to BigQuery table {dataset_id}.{table_id}")

        # Upload the DataFrame
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        full_table_id = f"{config['project_id']}.{dataset_id}.{table_id}"
        logger.info(f"Successfully uploaded data to {full_table_id}")

        return full_table_id

    except Exception as e:
        logger.error(f"Error uploading to BigQuery: {e}")
        raise


def create_vertex_dataset(config: Dict, bq_table_id: str, dataset_name: str) -> str:
    """Create a Vertex AI dataset from BigQuery table"""

    # Initialize Vertex AI
    aiplatform.init(
        project=config['project_id'],
        location=config['region']
    )

    try:
        logger.info(f"Creating Vertex AI dataset: {dataset_name}")

        # Create dataset with BigQuery source
        dataset = aiplatform.TabularDataset.create(
            display_name=dataset_name,
            bq_source=f"bq://{bq_table_id}",
            labels={"source": "weight_extraction", "type": "training"}
        )

        logger.info(f"Successfully created Vertex AI dataset: {dataset.display_name}")
        logger.info(f"Dataset resource name: {dataset.resource_name}")

        return dataset.resource_name

    except Exception as e:
        logger.error(f"Error creating Vertex AI dataset: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Clean CSV and create Vertex AI dataset')
    parser.add_argument('--csv-path', required=True, help='Path to the weight extraction CSV file')
    parser.add_argument('--config-path', default='config.yml', help='Path to config file')
    parser.add_argument('--dataset-name', default='weight-extraction-dataset', help='Name for Vertex AI dataset')
    parser.add_argument('--table-suffix', default='_training', help='Suffix for BigQuery table')
    parser.add_argument('--skip-vertex', action='store_true', help='Skip Vertex AI dataset creation')

    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config_path)

        # Clean the CSV data
        cleaned_df = clean_csv_data(args.csv_path)

        # Display some statistics
        logger.info("Dataset Statistics:")
        logger.info(f"  Total samples: {len(cleaned_df)}")
        logger.info(f"  Target range: {cleaned_df['target'].min():.2f} - {cleaned_df['target'].max():.2f} lbs")
        logger.info(f"  Average target: {cleaned_df['target'].mean():.2f} lbs")
        logger.info(f"  Extraction methods: {cleaned_df['extraction_method'].value_counts().to_dict()}")

        # Display URL filtering statistics if url column exists
        if 'url' in cleaned_df.columns:
            unique_domains = cleaned_df['url'].str.extract(r'https?://([^/]+)')[0].value_counts()
            logger.info(f"  Remaining domains: {unique_domains.head(10).to_dict()}")

        # Upload to BigQuery
        bq_table_id = upload_to_bigquery(cleaned_df, config, args.table_suffix)

        # Create Vertex AI dataset if not skipped
        if not args.skip_vertex:
            vertex_dataset_name = create_vertex_dataset(config, bq_table_id, args.dataset_name)
            logger.info(f"✅ Vertex AI dataset created: {vertex_dataset_name}")
        else:
            logger.info("⏭️  Skipped Vertex AI dataset creation")

        logger.info("✅ Process completed successfully!")

        # Save a sample of cleaned data for inspection
        sample_path = "cleaned_sample.csv"
        cleaned_df.head(100).to_csv(sample_path, index=False)
        logger.info(f"💾 Saved sample of cleaned data to {sample_path}")

    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise


if __name__ == "__main__":
    main()