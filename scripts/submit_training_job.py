from google.cloud import aiplatform
import yaml


def submit_training_job():
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    # Set staging bucket in aiplatform.init()
    staging_bucket = f"gs://{config['project_id']}-model-artifacts"

    aiplatform.init(
        project=config['project_id'],
        location=config['region'],
        staging_bucket=staging_bucket
    )

    # Arguments are passed in the command list, not as separate args parameter
    container_args = [
        "--project-id", config['project_id'],
        "--table-id", f"{config['project_id']}.{config['bq_dataset']}.{config['bq_table']}",
        "--bucket-name", f"{config['project_id']}-model-artifacts",
        "--epochs", "20",
        "--batch-size", "8"  # Conservative for T4 16GB memory
    ]

    job = aiplatform.CustomContainerTrainingJob(
        display_name="encoder-training-gpu",
        container_uri=f"{config['region']}-docker.pkg.dev/{config['project_id']}/ml-training/encoder-trainer:latest",
        command=["python", "src/train_encoder.py"] + container_args
    )

    job.run(
        replica_count=1,
        machine_type="n1-standard-4",  # Standard machine is fine with GPU
        accelerator_type="NVIDIA_TESLA_T4",
        accelerator_count=1,
        service_account=f"vertex-training-sa@{config['project_id']}.iam.gserviceaccount.com"
    )


if __name__ == "__main__":
    submit_training_job()