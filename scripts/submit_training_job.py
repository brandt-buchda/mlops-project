from google.cloud import aiplatform
import yaml


def submit_t5_training_job():
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    # Set staging bucket
    staging_bucket = f"gs://{config['project_id']}-model-artifacts"

    aiplatform.init(
        project=config['project_id'],
        location=config['region'],
        staging_bucket=staging_bucket
    )

    # Training arguments for T5 weight text generation
    container_args = [
        "--config-path", "config.yml",  # Use config file instead of individual params
        "--bucket-name", f"{config['project_id']}-model-artifacts",
        "--model-name", "t5-large",  # or "t5-base" for better performance
        "--model-path", "models/t5-weight-text",  # Updated model path
        "--epochs", "15",
        "--batch-size", "4",  # Small batch size for T5 on T4 GPU
        "--eval-batch-size", "8",
        "--lr", "3e-4",
        "--warmup-steps", "500",
        "--max-input-length", "512",
        "--max-target-length", "64",  # Increased for text generation
        "--eval-steps", "200"
    ]

    # Create the training job
    job = aiplatform.CustomContainerTrainingJob(
        display_name="t5-weight-text-generation-training",  # Updated job name
        container_uri=f"{config['region']}-docker.pkg.dev/{config['project_id']}/ml-training/t5-weight-trainer:latest"
    )

    # Submit the job with GPU
    job.run(
        args=container_args,
        replica_count=1,
        machine_type="n1-standard-4",
        accelerator_type="NVIDIA_TESLA_T4",
        accelerator_count=1,
        service_account=f"vertex-training-sa@{config['project_id']}.iam.gserviceaccount.com",
        sync=False  # Set to True if you want to wait for completion
    )

    print("✅ T5 weight text generation training job submitted to Vertex AI!")
    print("Check progress in the Vertex AI console:")
    print(f"https://console.cloud.google.com/vertex-ai/training/custom-jobs?project={config['project_id']}")


if __name__ == "__main__":
    submit_t5_training_job()