from kfp import dsl
from kfp.dsl import component, Input, Output, Model, Artifact
from typing import NamedTuple


@component(
    base_image="python:3.9",
    packages_to_install=[
        "google-cloud-aiplatform",
        "google-cloud-storage",
    ]
)
def model_deployment_component(
        model: Input[Model],
        endpoint_info: Output[Artifact],
        project_id: str,
        region: str,
        endpoint_display_name: str,
        model_display_name: str,
        serving_container_image: str = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-gpu.1-13:latest"
) -> NamedTuple('DeploymentInfo', [('endpoint_id', str), ('model_resource_name', str)]):
    """Deploy trained model to Vertex AI endpoint"""
    from google.cloud import aiplatform
    import json
    from collections import namedtuple

    # Initialize Vertex AI
    aiplatform.init(project=project_id, location=region)

    # Upload model to Vertex AI Model Registry
    uploaded_model = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=model.path,
        serving_container_image_uri=serving_container_image,
        serving_container_predict_route="/predict",
        serving_container_health_route="/health",
        description="T5 Weight Extraction Model"
    )

    print(f"Model uploaded: {uploaded_model.resource_name}")

    # Create endpoint
    endpoint = aiplatform.Endpoint.create(display_name=endpoint_display_name)
    print(f"Endpoint created: {endpoint.resource_name}")

    # Deploy model to endpoint
    deployed_model = endpoint.deploy(
        model=uploaded_model,
        machine_type="n1-standard-4",
        accelerator_type="NVIDIA_TESLA_T4",
        accelerator_count=1,
        min_replica_count=1,
        max_replica_count=3,
        traffic_split={"0": 100}
    )

    # Save endpoint information
    endpoint_data = {
        'endpoint_id': endpoint.resource_name.split('/')[-1],
        'endpoint_resource_name': endpoint.resource_name,
        'model_resource_name': uploaded_model.resource_name,
        'endpoint_display_name': endpoint_display_name
    }

    with open(endpoint_info.path, 'w') as f:
        json.dump(endpoint_data, f)

    print(f"Model deployed successfully to endpoint: {endpoint.resource_name}")

    DeploymentInfo = namedtuple('DeploymentInfo', ['endpoint_id', 'model_resource_name'])
    return DeploymentInfo(endpoint.resource_name.split('/')[-1], uploaded_model.resource_name)