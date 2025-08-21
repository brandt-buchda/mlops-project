from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset, Metrics
from typing import NamedTuple


@component(
    base_image="python:3.9",
    packages_to_install=[
        "google-cloud-bigquery",
        "pandas",
        "numpy",
        "scikit-learn",
        "google-auth",
        "db-dtypes"
    ]
)
def data_preprocessing_component(
        project_id: str,
        dataset_id: str,
        table_id: str,
        train_split: float,
        test_split: float,
        train_dataset: Output[Dataset],
        test_dataset: Output[Dataset],
        data_stats: Output[Metrics]
) -> NamedTuple('DataStats', [('train_samples', int), ('test_samples', int), ('total_samples', int)]):
    """Preprocess data and split into train/test sets"""
    import pandas as pd
    from google.cloud import bigquery
    import json
    from collections import namedtuple
    import os

    print(f"Starting preprocessing with project_id: {project_id}")
    print(f"Dataset: {dataset_id}, Table: {table_id}")

    try:
        # Initialize BigQuery client with explicit project
        client = bigquery.Client(project=project_id)

        # Test client authentication
        print(f"BigQuery client initialized for project: {client.project}")

        # Construct the full table reference
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        # Query to get training data
        query = f"""
            SELECT 
                name,
                variant,
                description,
                weight,
                extracted_weight,
                extracted_quantity,
                extraction_method,
                target,
                target_text
            FROM `{full_table_id}` 
            WHERE target IS NOT NULL 
            AND target_text IS NOT NULL
            AND target > 0
            ORDER BY RAND()
            LIMIT 10000
        """

        print(f"Executing query on table: {full_table_id}")
        print(f"Query: {query}")

        # Execute query with error handling
        query_job = client.query(query)
        df = query_job.to_dataframe()

        print(f"Successfully loaded {len(df)} samples from BigQuery")

        if len(df) == 0:
            raise ValueError(f"No data found in table {full_table_id}")

        # Split data
        train_size = int(train_split * len(df))
        test_size = len(df) - train_size

        train_df = df.iloc[:train_size]
        test_df = df.iloc[train_size:]

        print(f"Data split: {len(train_df)} train, {len(test_df)} test")

        # Save datasets
        train_df.to_csv(train_dataset.path, index=False)
        test_df.to_csv(test_dataset.path, index=False)

        # Create data statistics
        stats = {
            'total_samples': len(df),
            'train_samples': len(train_df),
            'test_samples': len(test_df),
            'target_weight_min': float(df['target'].min()),
            'target_weight_max': float(df['target'].max()),
            'target_weight_mean': float(df['target'].mean()),
            'unique_target_texts': int(df['target_text'].nunique()),
            'table_accessed': full_table_id
        }

        with open(data_stats.path, 'w') as f:
            json.dump(stats, f)

        print("✅ Preprocessing completed successfully")

        DataStats = namedtuple('DataStats', ['train_samples', 'test_samples', 'total_samples'])
        return DataStats(len(train_df), len(test_df), len(df))

    except Exception as e:
        print(f"❌ Error in preprocessing: {str(e)}")
        print(f"Error type: {type(e).__name__}")

        # Print additional debug info
        print(f"Project ID: {project_id}")
        print(f"Expected table: {project_id}.{dataset_id}.{table_id}")

        # Re-raise the error with more context
        raise Exception(f"BigQuery access failed: {str(e)}")