import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from transformers import T5ForConditionalGeneration, T5Tokenizer, AdamW, get_linear_schedule_with_warmup
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import argparse
import os
import json
import re
from tqdm import tqdm
import numpy as np


class WeightRegressionDataset(Dataset):
    def __init__(self, inputs, targets, tokenizer, max_input_length=512, max_target_length=32):
        self.inputs = inputs
        self.targets = targets
        self.tokenizer = tokenizer
        self.max_input_length = max_input_length
        self.max_target_length = max_target_length

    def __len__(self):
        return len(self.inputs)

    def __getitem__(self, idx):
        input_text = self.inputs[idx]
        target_weight = self.targets[idx]

        # Format target as text (e.g., "12.5" for 12.5 kg)
        target_text = f"{target_weight:.2f}"

        # Tokenize input
        input_encoding = self.tokenizer(
            input_text,
            truncation=True,
            padding='max_length',
            max_length=self.max_input_length,
            return_tensors='pt'
        )

        # Tokenize target
        target_encoding = self.tokenizer(
            target_text,
            truncation=True,
            padding='max_length',
            max_length=self.max_target_length,
            return_tensors='pt'
        )

        # Set -100 for padded tokens in labels (T5 requirement)
        labels = target_encoding['input_ids'].clone()
        labels[labels == self.tokenizer.pad_token_id] = -100

        return {
            'input_ids': input_encoding['input_ids'].flatten(),
            'attention_mask': input_encoding['attention_mask'].flatten(),
            'labels': labels.flatten(),
            'target_weight': torch.tensor(target_weight, dtype=torch.float32)
        }


def load_weight_data_from_bigquery(project_id, table_id):
    """Load weight regression data from BigQuery"""
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT name, variant, description, url, weight 
        FROM `{table_id}` 
        WHERE weight IS NOT NULL 
        AND name IS NOT NULL
        AND description IS NOT NULL
    """

    df = client.query(query).to_dataframe()

    # Create input text combining all product information
    inputs = []
    for _, row in df.iterrows():
        # Combine all available information with proper formatting
        parts = [f"predict weight:"]
        if pd.notna(row['name']):
            parts.append(f"name: {row['name']}")
        if pd.notna(row['variant']):
            parts.append(f"variant: {row['variant']}")
        if pd.notna(row['description']):
            parts.append(f"description: {row['description']}")
        if pd.notna(row['url']):
            parts.append(f"url: {row['url']}")

        input_text = " ".join(parts)
        inputs.append(input_text)

    weights = df['weight'].tolist()

    return inputs, weights


def extract_weight_from_text(text):
    """Extract numerical weight from generated text"""
    try:
        # Remove any non-numeric characters except decimal point
        cleaned = re.sub(r'[^\d.]', '', text.strip())
        if cleaned:
            return float(cleaned)
        return 0.0
    except:
        return 0.0


def evaluate_model(model, dataloader, tokenizer, device):
    """Evaluate T5 model for weight regression"""
    model.eval()
    total_loss = 0
    total_mae = 0
    total_mse = 0
    num_samples = 0

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)
            true_weights = batch['target_weight'].numpy()

            # Calculate loss
            outputs = model(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=labels
            )
            total_loss += outputs.loss.item()

            # Generate predictions for regression metrics
            generated_ids = model.generate(
                input_ids=input_ids,
                attention_mask=attention_mask,
                max_length=32,
                num_beams=1,
                do_sample=False
            )

            # Decode and extract weights
            predictions = []
            for generated_seq in generated_ids:
                generated_text = tokenizer.decode(generated_seq, skip_special_tokens=True)
                pred_weight = extract_weight_from_text(generated_text)
                predictions.append(pred_weight)

            predictions = np.array(predictions)

            # Calculate regression metrics
            mae = np.mean(np.abs(predictions - true_weights))
            mse = np.mean((predictions - true_weights) ** 2)

            total_mae += mae * len(true_weights)
            total_mse += mse * len(true_weights)
            num_samples += len(true_weights)

    avg_loss = total_loss / len(dataloader)
    avg_mae = total_mae / num_samples
    avg_mse = total_mse / num_samples
    avg_rmse = np.sqrt(avg_mse)

    return {
        'loss': avg_loss,
        'mae': avg_mae,
        'mse': avg_mse,
        'rmse': avg_rmse
    }


def save_model_to_gcs(model, tokenizer, bucket_name, model_path):
    """Save T5 model and tokenizer to Google Cloud Storage"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Save model locally first
    local_model_dir = '/tmp/t5_weight_model'
    os.makedirs(local_model_dir, exist_ok=True)

    # Save model and tokenizer
    model.save_pretrained(local_model_dir)
    tokenizer.save_pretrained(local_model_dir)

    # Upload to GCS
    for root, dirs, files in os.walk(local_model_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_model_dir)
            gcs_path = f"{model_path}/{relative_path}"

            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f"Uploaded {relative_path}")


def main():
    parser = argparse.ArgumentParser(description='Train T5 for Weight Regression')
    parser.add_argument('--project-id', required=True, help='GCP Project ID')
    parser.add_argument('--table-id', required=True, help='BigQuery table ID')
    parser.add_argument('--model-name', default='t5-small', help='T5 model variant')
    parser.add_argument('--bucket-name', required=True, help='GCS bucket name')
    parser.add_argument('--model-path', default='models/t5-weight', help='GCS model path')
    parser.add_argument('--epochs', type=int, default=5, help='Number of training epochs')
    parser.add_argument('--batch-size', type=int, default=8, help='Training batch size')
    parser.add_argument('--eval-batch-size', type=int, default=16, help='Evaluation batch size')
    parser.add_argument('--lr', type=float, default=3e-4, help='Learning rate')
    parser.add_argument('--warmup-steps', type=int, default=500, help='Warmup steps')
    parser.add_argument('--max-input-length', type=int, default=512, help='Max input length')
    parser.add_argument('--max-target-length', type=int, default=32, help='Max target length')
    parser.add_argument('--gradient-clip', type=float, default=1.0, help='Gradient clipping')
    parser.add_argument('--eval-steps', type=int, default=500, help='Evaluation frequency')

    args = parser.parse_args()

    # GPU diagnostics
    print("=" * 60)
    print("T5 WEIGHT REGRESSION TRAINING")
    print("=" * 60)
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"CUDA version: {torch.version.cuda}")
        print(f"GPU count: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            gpu_props = torch.cuda.get_device_properties(i)
            print(f"GPU {i}: {gpu_props.name}")
            print(f"  Memory: {gpu_props.total_memory / 1e9:.1f} GB")
        torch.cuda.empty_cache()
    print("=" * 60)

    # Load data
    print("Loading weight regression data from BigQuery...")
    inputs, weights = load_weight_data_from_bigquery(args.project_id, args.table_id)
    print(f"Loaded {len(inputs)} samples")
    print(f"Weight range: {min(weights):.2f} - {max(weights):.2f}")
    print(f"Average weight: {np.mean(weights):.2f}")

    # Split data (80/20)
    split_idx = int(0.8 * len(inputs))
    train_inputs, train_weights = inputs[:split_idx], weights[:split_idx]
    eval_inputs, eval_weights = inputs[split_idx:], weights[split_idx:]

    print(f"Training samples: {len(train_inputs)}")
    print(f"Evaluation samples: {len(eval_inputs)}")

    # Initialize tokenizer and model
    print(f"Loading T5 model: {args.model_name}")
    tokenizer = T5Tokenizer.from_pretrained(args.model_name)
    model = T5ForConditionalGeneration.from_pretrained(args.model_name)

    # Create datasets and dataloaders
    train_dataset = WeightRegressionDataset(
        train_inputs, train_weights, tokenizer,
        args.max_input_length, args.max_target_length
    )
    eval_dataset = WeightRegressionDataset(
        eval_inputs, eval_weights, tokenizer,
        args.max_input_length, args.max_target_length
    )

    train_dataloader = DataLoader(
        train_dataset, batch_size=args.batch_size, shuffle=True, num_workers=2
    )
    eval_dataloader = DataLoader(
        eval_dataset, batch_size=args.eval_batch_size, shuffle=False, num_workers=2
    )

    # Training setup
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Training device: {device}")
    model.to(device)

    # Optimizer and scheduler
    optimizer = AdamW(model.parameters(), lr=args.lr, eps=1e-8)

    total_steps = len(train_dataloader) * args.epochs
    scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=args.warmup_steps,
        num_training_steps=total_steps
    )

    print(f"Total training steps: {total_steps}")

    # Training loop
    model.train()
    global_step = 0
    best_eval_mae = float('inf')

    for epoch in range(args.epochs):
        print(f"\n=== Epoch {epoch + 1}/{args.epochs} ===")
        epoch_loss = 0
        epoch_steps = 0

        progress_bar = tqdm(train_dataloader, desc=f"Epoch {epoch + 1}")

        for batch in progress_bar:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)

            # Forward pass
            outputs = model(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=labels
            )

            loss = outputs.loss
            epoch_loss += loss.item()
            epoch_steps += 1

            # Backward pass
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), args.gradient_clip)
            optimizer.step()
            scheduler.step()
            optimizer.zero_grad()

            global_step += 1

            # Update progress bar
            progress_bar.set_postfix({
                'loss': f'{loss.item():.4f}',
                'avg_loss': f'{epoch_loss / epoch_steps:.4f}',
                'lr': f'{scheduler.get_last_lr()[0]:.2e}'
            })

            # Evaluation
            if global_step % args.eval_steps == 0:
                print(f"\nEvaluation at step {global_step}")
                eval_metrics = evaluate_model(model, eval_dataloader, tokenizer, device)

                print(f"Eval Loss: {eval_metrics['loss']:.4f}")
                print(f"Eval MAE: {eval_metrics['mae']:.4f}")
                print(f"Eval RMSE: {eval_metrics['rmse']:.4f}")

                if eval_metrics['mae'] < best_eval_mae:
                    best_eval_mae = eval_metrics['mae']
                    print(f"🎉 New best MAE: {best_eval_mae:.4f}")

                model.train()

            # Memory cleanup
            if torch.cuda.is_available() and global_step % 100 == 0:
                torch.cuda.empty_cache()

        # End of epoch evaluation
        avg_epoch_loss = epoch_loss / epoch_steps
        print(f"Epoch {epoch + 1} completed - Average Loss: {avg_epoch_loss:.4f}")

        eval_metrics = evaluate_model(model, eval_dataloader, tokenizer, device)
        print(f"End of Epoch - MAE: {eval_metrics['mae']:.4f}, RMSE: {eval_metrics['rmse']:.4f}")

    # Final evaluation
    print("\n" + "=" * 60)
    print("FINAL EVALUATION")
    print("=" * 60)
    final_metrics = evaluate_model(model, eval_dataloader, tokenizer, device)
    print(f"Final Loss: {final_metrics['loss']:.4f}")
    print(f"Final MAE: {final_metrics['mae']:.4f}")
    print(f"Final MSE: {final_metrics['mse']:.4f}")
    print(f"Final RMSE: {final_metrics['rmse']:.4f}")

    # Save model to GCS
    print("\nSaving model to GCS...")
    save_model_to_gcs(model, tokenizer, args.bucket_name, args.model_path)

    # Save training config
    config = {
        'model_name': args.model_name,
        'task_type': 'weight_regression',
        'epochs': args.epochs,
        'batch_size': args.batch_size,
        'learning_rate': args.lr,
        'max_input_length': args.max_input_length,
        'max_target_length': args.max_target_length,
        'best_eval_mae': best_eval_mae,
        'final_metrics': final_metrics,
        'total_training_samples': len(train_inputs),
        'total_eval_samples': len(eval_inputs),
        'weight_stats': {
            'min_weight': float(min(weights)),
            'max_weight': float(max(weights)),
            'avg_weight': float(np.mean(weights)),
            'std_weight': float(np.std(weights))
        }
    }

    # Save config to GCS
    client = storage.Client()
    bucket = client.bucket(args.bucket_name)
    config_blob = bucket.blob(f"{args.model_path}/training_config.json")
    config_blob.upload_from_string(json.dumps(config, indent=2))

    print(f"✅ T5 weight regression model saved to gs://{args.bucket_name}/{args.model_path}")
    print("✅ Training completed successfully!")


if __name__ == "__main__":
    main()