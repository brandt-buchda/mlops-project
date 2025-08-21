
from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset, Model, Metrics
from typing import NamedTuple


@component(
    base_image="pytorch/pytorch:2.0.1-cuda11.7-cudnn8-devel",
    packages_to_install=[
        "transformers==4.30.0",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "pandas",
        "numpy",
        "scikit-learn",
        "tqdm",
        "pyyaml",
        "sentencepiece",
    ]
)
def model_training_component(
        train_dataset: Input[Dataset],
        test_dataset: Input[Dataset],
        model_output: Output[Model],
        training_metrics: Output[Metrics],
        project_id: str,
        bucket_name: str,
        model_name: str = "t5-small",
        epochs: int = 3,
        batch_size: int = 4,
        learning_rate: float = 3e-4,
        max_input_length: int = 512,
        max_target_length: int = 64
) -> NamedTuple('TrainingResults', [('final_loss', float), ('rmse', float), ('mape', float)]):
    """Train T5 model for weight extraction"""
    import torch
    import torch.nn as nn
    from torch.utils.data import Dataset, DataLoader
    from transformers import T5ForConditionalGeneration, T5Tokenizer, AdamW
    import pandas as pd
    import numpy as np
    import json
    import re
    import os
    from collections import namedtuple
    from tqdm import tqdm

    # Custom Dataset class
    class WeightExtractionDataset(Dataset):
        def __init__(self, data_df, tokenizer, max_input_length=512, max_target_length=64):
            self.data = data_df.reset_index(drop=True)
            self.tokenizer = tokenizer
            self.max_input_length = max_input_length
            self.max_target_length = max_target_length

        def __len__(self):
            return len(self.data)

        def __getitem__(self, idx):
            row = self.data.iloc[idx]

            # Create input text
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

            input_text = " ".join(input_parts)
            target_text = row['target_text']

            # Tokenize
            input_encoding = self.tokenizer(
                input_text,
                truncation=True,
                padding='max_length',
                max_length=self.max_input_length,
                return_tensors='pt'
            )

            target_encoding = self.tokenizer(
                target_text,
                truncation=True,
                padding='max_length',
                max_length=self.max_target_length,
                return_tensors='pt'
            )

            labels = target_encoding['input_ids'].clone()
            labels[labels == self.tokenizer.pad_token_id] = -100

            return {
                'input_ids': input_encoding['input_ids'].flatten(),
                'attention_mask': input_encoding['attention_mask'].flatten(),
                'labels': labels.flatten(),
                'target_text': target_text,
                'target_weight': float(row['target'])
            }

    def parse_target_text(text: str):
        """Parse target_text to extract weight"""
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

    def calculate_metrics(predictions, targets, target_weights):
        """Calculate MAPE and RMSE"""
        valid_predictions = []
        valid_targets = []

        for pred, target_weight in zip(predictions, target_weights):
            pred_weight = parse_target_text(pred)
            if pred_weight is not None:
                valid_predictions.append(pred_weight)
                valid_targets.append(target_weight)

        if len(valid_predictions) == 0:
            return float('inf'), float('inf')

        valid_predictions = np.array(valid_predictions)
        valid_targets = np.array(valid_targets)

        # RMSE
        rmse = np.sqrt(np.mean((valid_predictions - valid_targets) ** 2))

        # MAPE (avoiding division by zero)
        mape = np.mean(np.abs((valid_targets - valid_predictions) / np.maximum(valid_targets, 1e-8))) * 100

        return rmse, mape

    # Load datasets
    train_df = pd.read_csv(train_dataset.path)
    test_df = pd.read_csv(test_dataset.path)

    print(f"Training samples: {len(train_df)}")
    print(f"Test samples: {len(test_df)}")

    # Initialize model and tokenizer
    tokenizer = T5Tokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)

    # Setup device
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    print(f"Using device: {device}")

    # Create datasets and dataloaders
    train_dataset_obj = WeightExtractionDataset(train_df, tokenizer, max_input_length, max_target_length)
    test_dataset_obj = WeightExtractionDataset(test_df, tokenizer, max_input_length, max_target_length)

    train_dataloader = DataLoader(train_dataset_obj, batch_size=batch_size, shuffle=True, num_workers=0)
    test_dataloader = DataLoader(test_dataset_obj, batch_size=batch_size, shuffle=False, num_workers=0)

    # Training setup
    optimizer = AdamW(model.parameters(), lr=learning_rate)

    # Training loop
    model.train()
    final_loss = 0

    for epoch in range(epochs):
        epoch_loss = 0
        num_batches = 0

        for batch in tqdm(train_dataloader, desc=f"Epoch {epoch + 1}"):
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)

            optimizer.zero_grad()
            outputs = model(input_ids=input_ids, attention_mask=attention_mask, labels=labels)
            loss = outputs.loss
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()
            num_batches += 1

        avg_loss = epoch_loss / num_batches
        final_loss = avg_loss
        print(f"Epoch {epoch + 1}/{epochs}, Average Loss: {avg_loss:.4f}")

    # Evaluation on test set
    model.eval()
    predictions = []
    targets = []
    target_weights = []

    with torch.no_grad():
        for batch in tqdm(test_dataloader, desc="Evaluating"):
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)

            generated_ids = model.generate(
                input_ids=input_ids,
                attention_mask=attention_mask,
                max_length=max_target_length,
                num_beams=2,
                do_sample=False,
                early_stopping=True
            )

            batch_predictions = [tokenizer.decode(ids, skip_special_tokens=True) for ids in generated_ids]
            predictions.extend(batch_predictions)
            targets.extend(batch['target_text'])
            target_weights.extend([float(w) for w in batch['target_weight']])

    # Calculate metrics
    rmse, mape = calculate_metrics(predictions, targets, target_weights)

    # Save model
    model.save_pretrained(model_output.path)
    tokenizer.save_pretrained(model_output.path)

    # Save metrics
    metrics_data = {
        'final_loss': final_loss,
        'rmse': rmse,
        'mape': mape,
        'sample_predictions': predictions[:5],
        'sample_targets': targets[:5]
    }

    with open(training_metrics.path, 'w') as f:
        json.dump(metrics_data, f)

    print(f"Training completed. Final Loss: {final_loss:.4f}, RMSE: {rmse:.4f}, MAPE: {mape:.2f}%")

    TrainingResults = namedtuple('TrainingResults', ['final_loss', 'rmse', 'mape'])
    return TrainingResults(final_loss, rmse, mape)