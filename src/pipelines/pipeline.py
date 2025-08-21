from kfp import dsl, compiler
from kfp.dsl import pipeline
import yaml
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Import components using absolute imports
from src.pipelines.components.preprocessing import data_preprocessing_component
from src.pipelines.components.training import model_training_component
from src.pipelines.components.deployment import model_deployment_component
from src.pipelines.components.monitoring import setup_model_monitoring_component, test_deployed_model_component


@pipeline(
    name="weight-extraction-t5-pipeline",
    description="Complete MLOps pipeline for T5 weight extraction model with enhanced monitoring",
    pipeline_root="gs://mlops-466819-model-artifacts/pipeline_root"
)
def weight_extraction_pipeline(
        project_id: str = "mlops-466819",
        region: str = "us-central1",
        dataset_id: str = "gfv_dataset",
        table_id: str = "gfv_data_hashed",
        bucket_name: str = "mlops-466819-model-artifacts",
        model_name: str = "t5-large",  # Updated to t5-large as you mentioned
        epochs: int = 5,  # Increased for better performance
        batch_size: int = 4,
        learning_rate: float = 3e-4,
        endpoint_display_name: str = "weight-extraction-endpoint",
        model_display_name: str = "t5-weight-extraction",
        train_split: float = 0.8,
        test_split: float = 0.2
):
    """
    Enhanced MLOps pipeline for T5 weight extraction model including:
    - Data preprocessing and splitting
    - Model training with T5-large
    - Model deployment to Vertex AI endpoint
    - Comprehensive model monitoring setup
    - Testing with original data
    - Testing with quantity-restricted data (drift simulation)
    - Testing with description-modified data (additional drift simulation)
    """

    # Step 1: Data preprocessing and splitting
    preprocessing_task = data_preprocessing_component(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        train_split=train_split,
        test_split=test_split
    )

    # Step 2: Model training with T5-large
    training_task = model_training_component(
        train_dataset=preprocessing_task.outputs["train_dataset"],
        test_dataset=preprocessing_task.outputs["test_dataset"],
        project_id=project_id,
        bucket_name=bucket_name,
        model_name=model_name,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate
    )

    # Set GPU and resource requirements for training T5-large
    training_task.set_accelerator_type("NVIDIA_TESLA_T4")
    training_task.set_accelerator_limit(1)
    training_task.set_cpu_limit("4")
    training_task.set_memory_limit("16G")

    # Step 3: Model deployment
    deployment_task = model_deployment_component(
        model=training_task.outputs["model_output"],
        project_id=project_id,
        region=region,
        endpoint_display_name=endpoint_display_name,
        model_display_name=model_display_name
    )

    # Step 4: Setup enhanced model monitoring
    monitoring_task = setup_model_monitoring_component(
        endpoint_info=deployment_task.outputs["endpoint_info"],
        test_dataset=preprocessing_task.outputs["test_dataset"],
        project_id=project_id,
        region=region
    )

    # Step 5: Test deployed model with original test data
    test_original_task = test_deployed_model_component(
        endpoint_info=deployment_task.outputs["endpoint_info"],
        test_dataset=preprocessing_task.outputs["test_dataset"],
        project_id=project_id,
        region=region,
        restrict_quantity=False,
    )
    test_original_task.set_display_name("Test Model - Original Data")

    # Step 6: Test deployed model with quantity-restricted data
    test_quantity_restricted_task = test_deployed_model_component(
        endpoint_info=deployment_task.outputs["endpoint_info"],
        test_dataset=preprocessing_task.outputs["test_dataset"],
        project_id=project_id,
        region=region,
        restrict_quantity=True,  # This restricts to quantity=1 products
    )
    test_quantity_restricted_task.set_display_name("Test Model - Quantity Restricted (Drift Test)")

    # Step 7: Test deployed model with description modifications
    test_description_modified_task = test_deployed_model_component(
        endpoint_info=deployment_task.outputs["endpoint_info"],
        test_dataset=preprocessing_task.outputs["test_dataset"],
        project_id=project_id,
        region=region,
        restrict_quantity=False,
    )
    test_description_modified_task.set_display_name("Test Model - Description Modified (Drift Test)")

    # Set dependencies to ensure proper execution order
    training_task.after(preprocessing_task)
    deployment_task.after(training_task)
    monitoring_task.after(deployment_task)
    test_original_task.after(deployment_task)
    test_quantity_restricted_task.after(deployment_task)
    test_description_modified_task.after(deployment_task)


def compile_pipeline():
    """Compile the enhanced pipeline"""
    compiler.Compiler().compile(
        pipeline_func=weight_extraction_pipeline,
        package_path="weight_extraction_pipeline.json"
    )
    print("✅ Enhanced pipeline compiled successfully: weight_extraction_pipeline.json")
    print("\n📋 Pipeline features:")
    print("   • Data preprocessing and train/test split")
    print("   • T5-large model training with GPU optimization")
    print("   • MAPE and RMSE evaluation metrics")
    print("   • Vertex AI endpoint deployment")
    print("   • Enhanced model monitoring with drift detection")
    print("   • Original data testing (baseline)")
    print("   • Quantity-restricted testing (simulates distribution shift)")
    print("   • Description-modified testing (simulates feature drift)")


if __name__ == "__main__":
    compile_pipeline()