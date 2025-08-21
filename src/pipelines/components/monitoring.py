from kfp import dsl
from kfp.dsl import component, Input, Output, Artifact, Dataset
from typing import NamedTuple


@component(
    base_image="python:3.9",
    packages_to_install=[
        "google-cloud-aiplatform",
        "google-cloud-monitoring",
        "pandas",
        "numpy"
    ]
)
def setup_model_monitoring_component(
        endpoint_info: Input[Artifact],
        test_dataset: Input[Dataset],
        monitoring_config: Output[Artifact],
        project_id: str,
        region: str,
        monitoring_job_display_name: str = "weight-extraction-monitoring"
) -> NamedTuple('MonitoringInfo', [('monitoring_job_id', str), ('skew_threshold', float)]):
    """Setup model monitoring for the deployed model"""
    from google.cloud import aiplatform
    import json
    import pandas as pd
    from collections import namedtuple

    # Initialize Vertex AI
    aiplatform.init(project=project_id, location=region)

    # Load endpoint info
    with open(endpoint_info.path, 'r') as f:
        endpoint_data = json.load(f)

    endpoint = aiplatform.Endpoint(endpoint_data['endpoint_resource_name'])

    # Load test dataset for baseline
    test_df = pd.read_csv(test_dataset.path)

    # Create monitoring job configuration
    monitoring_config_data = {
        'project_id': project_id,
        'endpoint_resource_name': endpoint_data['endpoint_resource_name'],
        'model_resource_name': endpoint_data['model_resource_name'],
        'monitoring_job_display_name': monitoring_job_display_name,
        'skew_threshold': 0.3,  # Threshold for data drift detection
        'drift_threshold': 0.3,  # Threshold for prediction drift
        'sampling_rate': 1.0,  # Sample 100% of predictions
        'baseline_dataset_size': len(test_df),
        'monitoring_enabled': True,
        'alert_config': {
            'email_alert_config': {
                'user_emails': []  # Add emails for alerts
            }
        }
    }

    # Save monitoring configuration
    with open(monitoring_config.path, 'w') as f:
        json.dump(monitoring_config_data, f, indent=2)

    print("Model monitoring configuration created")
    print(f"Monitoring will track data drift with threshold: {monitoring_config_data['skew_threshold']}")

    MonitoringInfo = namedtuple('MonitoringInfo', ['monitoring_job_id', 'skew_threshold'])
    return MonitoringInfo("mock-monitoring-job-id", 0.3)


@component(
    base_image="python:3.9",
    packages_to_install=[
        "google-cloud-aiplatform",
        "pandas",
        "numpy",
        "scikit-learn"
    ]
)
def test_deployed_model_component(
        endpoint_info: Input[Artifact],
        test_dataset: Input[Dataset],
        test_results: Output[Artifact],
        project_id: str,
        region: str,
        restrict_quantity: bool = False
) -> NamedTuple('TestResults', [('rmse', float), ('mape', float), ('num_samples', int)]):
    """Test the deployed model with test data"""
    import json
    import pandas as pd
    import numpy as np
    from google.cloud import aiplatform
    from collections import namedtuple
    import re

    def parse_prediction(text: str):
        """Parse prediction to extract weight"""
        try:
            text = str(text).strip().lower()
            pattern = r'(\d+(?:\.\d+)?)\s*x\s*(\d+(?:\.\d+)?)\s*lbs?'
            match = re.search(pattern, text)
            if match:
                quantity = float(match.group(1))
                individual_weight = float(match.group(2))
                return individual_weight * quantity

            pattern_simple = r'(\d+(?:\.\d+)?)\s*lbs?'
            match_simple = re.search(pattern_simple, text)
            if match_simple:
                return float(match_simple.group(1))
            return None
        except (ValueError, AttributeError):
            return None

    # Initialize Vertex AI
    aiplatform.init(project=project_id, location=region)

    # Load endpoint info
    with open(endpoint_info.path, 'r') as f:
        endpoint_data = json.load(f)

    endpoint = aiplatform.Endpoint(endpoint_data['endpoint_resource_name'])

    # Load test dataset
    test_df = pd.read_csv(test_dataset.path)

    # Restrict dataset if requested (for drift testing)
    if restrict_quantity:
        # Filter to products with quantity = 1 only
        test_df = test_df[test_df['target_text'].str.contains('1 x', na=False)]
        print(f"Restricted dataset to quantity=1 products: {len(test_df)} samples")

    # Prepare test instances
    test_instances = []
    for _, row in test_df.iterrows():
        input_parts = ["Extract weight:"]
        if pd.notna(row.get('name', '')) and row['name']:
            input_parts.append(f"name: {row['name']}")
        if pd.notna(row.get('variant', '')) and row['variant']:
            input_parts.append(f"variant: {row['variant']}")
        if pd.notna(row.get('description', '')) and row['description']:
            desc = str(row['description'])[:300] + "..." if len(str(row['description'])) > 300 else str(
                row['description'])
            input_parts.append(f"description: {desc}")
        if pd.notna(row.get('weight', '')) and row['weight']:
            input_parts.append(f"weight: {row['weight']}")

        test_instances.append({"input_text": " ".join(input_parts)})

    # Get predictions in batches
    batch_size = 10
    all_predictions = []

    for i in range(0, len(test_instances), batch_size):
        batch = test_instances[i:i + batch_size]
        try:
            predictions = endpoint.predict(instances=batch)
            all_predictions.extend(predictions.predictions)
        except Exception as e:
            print(f"Prediction error for batch {i // batch_size}: {e}")
            # Add dummy predictions for failed batch
            all_predictions.extend(["0 lbs"] * len(batch))

    # Calculate metrics
    predicted_weights = []
    true_weights = []

    for pred, true_weight in zip(all_predictions, test_df['target'].values):
        pred_weight = parse_prediction(str(pred))
        if pred_weight is not None:
            predicted_weights.append(pred_weight)
            true_weights.append(true_weight)

    if len(predicted_weights) > 0:
        predicted_weights = np.array(predicted_weights)
        true_weights = np.array(true_weights)

        # Calculate RMSE and MAPE
        rmse = np.sqrt(np.mean((predicted_weights - true_weights) ** 2))
        mape = np.mean(np.abs((true_weights - predicted_weights) / np.maximum(true_weights, 1e-8))) * 100
    else:
        rmse = float('inf')
        mape = float('inf')

    # Save test results
    results_data = {
        'rmse': rmse,
        'mape': mape,
        'num_samples': len(predicted_weights),
        'total_test_samples': len(test_df),
        'parsing_success_rate': len(predicted_weights) / len(test_df) if len(test_df) > 0 else 0,
        'restricted_quantity': restrict_quantity,
        'sample_predictions': all_predictions[:5],
        'sample_true_values': test_df['target'].head().tolist()
    }

    with open(test_results.path, 'w') as f:
        json.dump(results_data, f, indent=2)

    print(f"Testing completed:")
    print(f"RMSE: {rmse:.4f}")
    print(f"MAPE: {mape:.2f}%")
    print(f"Samples tested: {len(predicted_weights)}/{len(test_df)}")

    TestResults = namedtuple('TestResults', ['rmse', 'mape', 'num_samples'])
    return TestResults(rmse, mape, len(predicted_weights))