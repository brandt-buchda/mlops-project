
from google.cloud import aiplatform
import yaml
import os


def run_weight_extraction_pipeline():
    """Run the weight extraction pipeline on Vertex AI"""

    # Load configuration
    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)

    # Initialize Vertex AI
    aiplatform.init(
        project=config['project_id'],
        location=config['region']
    )

    # Create pipeline job with service account
    job = aiplatform.PipelineJob(
        display_name="weight-extraction-t5-pipeline-run",
        template_path="weight_extraction_pipeline.json",
        job_id=f"weight-extraction-{aiplatform.utils.timestamped_unique_name()}",
        parameter_values={
            "project_id": config['project_id'],
            "region": config['region'],
            "dataset_id": config['bq_dataset'],
            "table_id": config['bq_table'],
            "bucket_name": f"{config['project_id']}-model-artifacts",
            "model_name": "t5-small",
            "epochs": 3,
            "batch_size": 4,
            "learning_rate": 3e-4,
            "endpoint_display_name": "weight-extraction-endpoint",
            "model_display_name": "t5-weight-extraction",
            "train_split": 0.8,
            "test_split": 0.2
        },
        enable_caching=True
    )

    # Submit with service account
    job.submit(
        service_account=f"vertex-training-sa@{config['project_id']}.iam.gserviceaccount.com"
    )

    print("✅ Pipeline submitted with custom service account")
    return job


if __name__ == "__main__":
    job = run_weight_extraction_pipeline()