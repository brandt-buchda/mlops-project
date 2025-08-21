import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    AdamW,
    get_linear_schedule_with_warmup,
    DataCollatorForLanguageModeling
)
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import argparse
import os
import json
import re
from tqdm import tqdm
import numpy as np
import yaml
from typing import Dict, Tuple, Optional
import gc


def clear_gpu_memory():
    """Comprehensive GPU memory cleanup"""
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.synchronize()
        gc.collect()

        # Print memory stats
        allocated = torch.cuda.memory_allocated() / 1024 ** 3
        cached = torch.cuda.memory_reserved() / 1024 ** 3
        print(f"GPU Memory - Allocated: {allocated:.2f}GB, Cached: {cached:.2f}GB")


class WeightExtractionDataset(Dataset):
    def __init__(self, data_df, tokenizer, max_length=512):
        self.data = data_df.reset_index(drop=True)
        self.tokenizer = tokenizer
        self.max_length = max_length
        self.examples = self._prepare_examples()

    def _prepare_examples(self):
        """Prepare examples in a conversational format for decoder-only models"""
        examples = []

        for _, row in self.data.iterrows():
            # Create a conversational format for weight extraction
            input_parts = []

            if pd.notna(row.get('name', '')) and row['name']:
                input_parts.append(f"Product: {row['name']}")
            if pd.notna(row.get('variant', '')) and row['variant']:
                input_parts.append(f"Variant: {row['variant']}")
            if pd.notna(row.get('description', '')) and row['description']:
                # Truncate description if too long
                desc = str(row['description'])[:300] + "..." if len(str(row['description'])) > 300 else str(
                    row['description'])
                input_parts.append(f"Description: {desc}")
            if pd.notna(row.get('weight', '')) and row['weight']:
                input_parts.append(f"Weight field: {row['weight']}")

            input_text = "\n".join(input_parts)

            # Format as a conversation/instruction following task
            prompt = f"<|user|>\nExtract and format the weight information from this product data:\n\n{input_text}\n\nPlease provide the weight in the format 'quantity x individual_weight lbs' (e.g., '2 x 1.5 lbs').\n<|assistant|>\n{row['target_text']}<|endoftext|>"

            examples.append({
                'text': prompt,
                'target_text': row['target_text'],
                'target_weight': float(row['target'])
            })

        return examples

    def __len__(self):
        return len(self.examples)

    def __getitem__(self, idx):
        example = self.examples[idx]

        # Tokenize the full conversation
        encoding = self.tokenizer(
            example['text'],
            truncation=True,
            padding='max_length',
            max_length=self.max_length,
            return_tensors='pt'
        )

        # For causal LM training, labels are the same as input_ids
        # We'll mask the user part and only compute loss on assistant response
        input_ids = encoding['input_ids'].flatten()
        attention_mask = encoding['attention_mask'].flatten()

        # Create labels with -100 for tokens we don't want to compute loss on
        labels = input_ids.clone()

        # Find the assistant response start
        text = example['text']
        assistant_start = text.find('<|assistant|>\n')

        if assistant_start != -1:
            # Tokenize up to assistant response to find where to start computing loss
            prefix = text[:assistant_start + len('<|assistant|>\n')]
            prefix_tokens = self.tokenizer(prefix, add_special_tokens=False)['input_ids']

            # Set labels to -100 for everything before assistant response
            if len(prefix_tokens) < len(labels):
                labels[:len(prefix_tokens)] = -100

        return {
            'input_ids': input_ids,
            'attention_mask': attention_mask,
            'labels': labels,
            'target_text': example['target_text'],
            'target_weight': example['target_weight']
        }


def load_config(config_path: str = "config.yml") -> Dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def load_weight_data_from_bigquery(project_id: str, table_id: str) -> pd.DataFrame:
    """Load weight extraction data from BigQuery"""
    client = bigquery.Client(project=project_id)

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
        FROM `{table_id}` 
        WHERE target IS NOT NULL 
        AND target_text IS NOT NULL
        AND target > 0
        ORDER BY RAND()
    """

    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df)} samples from BigQuery")
    return df


def parse_target_text(text: str) -> Tuple[Optional[float], Optional[int]]:
    """
    Parse target_text to extract quantity and weight
    Expected format: "quantity x weight lbs" e.g., "2 x 1.5 lbs"
    Returns: (individual_weight, quantity)
    """
    try:
        # Clean up the text
        text = str(text).strip().lower()

        # Pattern to match "quantity x weight lbs"
        pattern = r'(\d+(?:\.\d+)?)\s*x\s*(\d+(?:\.\d+)?)\s*lbs?'
        match = re.search(pattern, text)

        if match:
            quantity = float(match.group(1))
            individual_weight = float(match.group(2))
            return individual_weight, int(quantity)

        # Fallback: look for just a number followed by lbs
        pattern_simple = r'(\d+(?:\.\d+)?)\s*lbs?'
        match_simple = re.search(pattern_simple, text)
        if match_simple:
            weight = float(match_simple.group(1))
            return weight, 1

        return None, None

    except (ValueError, AttributeError):
        return None, None


def calculate_total_weight_from_prediction(prediction: str) -> Optional[float]:
    """Calculate total weight from a prediction string"""
    individual_weight, quantity = parse_target_text(prediction)
    if individual_weight is not None and quantity is not None:
        return individual_weight * quantity
    return None


def extract_assistant_response(generated_text: str, original_prompt: str) -> str:
    """Extract just the assistant's response from the generated text"""
    # Remove the original prompt from the generated text
    if original_prompt in generated_text:
        response = generated_text[len(original_prompt):].strip()
    else:
        # Fallback: look for assistant marker
        assistant_marker = '<|assistant|>\n'
        if assistant_marker in generated_text:
            response = generated_text.split(assistant_marker)[-1]
        else:
            response = generated_text

    # Clean up the response
    response = response.replace('<|endoftext|>', '').strip()
    return response


def evaluate_model(model, dataloader, tokenizer, device, max_batches=None):
    """Evaluate decoder model for weight extraction"""
    model.eval()
    total_loss = 0
    total_exact_matches = 0
    total_weight_mae = 0
    total_weight_mse = 0
    total_parse_success = 0
    num_samples = 0

    predictions = []
    true_targets = []
    true_weights = []
    predicted_weights = []

    clear_gpu_memory()

    with torch.no_grad():
        for batch_idx, batch in enumerate(dataloader):
            if max_batches and batch_idx >= max_batches:
                break

            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)
            batch_true_weights = [float(w) for w in batch['target_weight']]
            batch_true_targets = batch['target_text']

            # Calculate loss
            outputs = model(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=labels
            )
            total_loss += outputs.loss.item()

            # Generate predictions
            # First, find where the assistant response should start
            batch_predictions = []
            batch_predicted_weights = []

            for i in range(input_ids.shape[0]):
                # Get the input text for this example
                input_text = tokenizer.decode(input_ids[i], skip_special_tokens=False)

                # Find where to start generation (after <|assistant|>)
                assistant_pos = input_text.find('<|assistant|>\n')
                if assistant_pos != -1:
                    prompt_end_pos = assistant_pos + len('<|assistant|>\n')
                    prompt = input_text[:prompt_end_pos]

                    # Tokenize just the prompt part
                    prompt_tokens = tokenizer(prompt, return_tensors='pt', add_special_tokens=False)
                    prompt_input_ids = prompt_tokens['input_ids'].to(device)
                    prompt_attention_mask = prompt_tokens['attention_mask'].to(device)

                    # Generate from the prompt
                    generated_ids = model.generate(
                        input_ids=prompt_input_ids,
                        attention_mask=prompt_attention_mask,
                        max_new_tokens=32,  # Keep response short
                        num_beams=2,
                        do_sample=False,
                        early_stopping=True,
                        pad_token_id=tokenizer.pad_token_id,
                        eos_token_id=tokenizer.eos_token_id
                    )

                    # Decode the generated response
                    generated_text = tokenizer.decode(generated_ids[0], skip_special_tokens=False)

                    # Extract just the assistant's response
                    assistant_response = extract_assistant_response(generated_text, prompt)

                else:
                    # Fallback: use the original approach
                    generated_ids = model.generate(
                        input_ids=input_ids[i:i + 1],
                        attention_mask=attention_mask[i:i + 1],
                        max_new_tokens=16,
                        num_beams=2,
                        do_sample=False,
                        pad_token_id=tokenizer.pad_token_id
                    )
                    assistant_response = tokenizer.decode(generated_ids[0], skip_special_tokens=True)

                batch_predictions.append(assistant_response)

                # Calculate weight from prediction
                pred_weight = calculate_total_weight_from_prediction(assistant_response)
                batch_predicted_weights.append(pred_weight)

                # Check exact match
                if assistant_response.strip().lower() == batch_true_targets[i].strip().lower():
                    total_exact_matches += 1

                # Track parsing success
                if pred_weight is not None:
                    total_parse_success += 1
                    weight_error = abs(pred_weight - batch_true_weights[i])
                    total_weight_mae += weight_error
                    total_weight_mse += weight_error ** 2
                else:
                    total_weight_mae += batch_true_weights[i]
                    total_weight_mse += batch_true_weights[i] ** 2

            predictions.extend(batch_predictions)
            true_targets.extend(batch_true_targets)
            true_weights.extend(batch_true_weights)
            predicted_weights.extend(batch_predicted_weights)
            num_samples += len(batch_true_weights)

            # Clean up
            del input_ids, attention_mask, labels, outputs

            if batch_idx % 10 == 0:
                clear_gpu_memory()

    avg_loss = total_loss / max(len(dataloader) if not max_batches else min(max_batches, len(dataloader)), 1)
    exact_match_accuracy = total_exact_matches / max(num_samples, 1)
    parse_success_rate = total_parse_success / max(num_samples, 1)
    avg_weight_mae = total_weight_mae / max(num_samples, 1)
    avg_weight_rmse = np.sqrt(total_weight_mse / max(num_samples, 1))

    return {
        'loss': avg_loss,
        'exact_match_accuracy': exact_match_accuracy,
        'parse_success_rate': parse_success_rate,
        'weight_mae': avg_weight_mae,
        'weight_rmse': avg_weight_rmse,
        'predictions': predictions[:10],
        'true_targets': true_targets[:10],
        'num_samples': num_samples
    }


def save_model_to_gcs(model, tokenizer, bucket_name, model_path):
    """Save decoder model and tokenizer to Google Cloud Storage"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Save model locally first
    local_model_dir = '/tmp/decoder_weight_model'
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

    # Save to Vertex AI model directory if available
    aip_model_dir = os.environ.get('AIP_MODEL_DIR')
    if aip_model_dir:
        print(f"Saving to Vertex AI model directory: {aip_model_dir}")
        model.save_pretrained(aip_model_dir)
        tokenizer.save_pretrained(aip_model_dir)

        config_data = {
            'model_name': model.config.name_or_path if hasattr(model.config, 'name_or_path') else 'unknown',
            'task_type': 'weight_text_generation_decoder',
            'framework': 'transformers',
            'model_type': 'AutoModelForCausalLM'
        }

        config_path = os.path.join(aip_model_dir, 'vertex_config.json')
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)

        print("✅ Model saved to Vertex AI model directory")


def main():
    parser = argparse.ArgumentParser(description='Train Decoder Model for Weight Text Generation')
    parser.add_argument('--config-path', default='config.yml', help='Path to config file')
    parser.add_argument('--model-name', default='Qwen/Qwen2-0.5B',
                        help='Decoder model name (try Qwen/Qwen2-0.5B, microsoft/DialoGPT-small, or distilgpt2)')
    parser.add_argument('--bucket-name', required=True, help='GCS bucket name')
    parser.add_argument('--model-path', default='models/decoder-weight', help='GCS model path')
    parser.add_argument('--epochs', type=int, default=5, help='Number of training epochs')
    parser.add_argument('--batch-size', type=int, default=4, help='Training batch size')
    parser.add_argument('--eval-batch-size', type=int, default=6, help='Evaluation batch size')
    parser.add_argument('--lr', type=float, default=3e-4, help='Learning rate')
    parser.add_argument('--warmup-steps', type=int, default=300, help='Warmup steps')
    parser.add_argument('--max-length', type=int, default=512, help='Max sequence length')
    parser.add_argument('--gradient-clip', type=float, default=1.0, help='Gradient clipping')
    parser.add_argument('--eval-steps', type=int, default=500, help='Evaluation frequency')
    parser.add_argument('--gradient-accumulation-steps', type=int, default=4, help='Gradient accumulation steps')
    parser.add_argument('--max-eval-batches', type=int, default=30, help='Max batches for evaluation')

    args = parser.parse_args()

    clear_gpu_memory()

    # Load configuration
    config = load_config(args.config_path)
    table_id = f"{config['project_id']}.{config['bq_dataset']}.{config['bq_table']}_training"

    # GPU diagnostics
    print("=" * 60)
    print("DECODER WEIGHT TEXT GENERATION TRAINING")
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
        clear_gpu_memory()
    print("=" * 60)

    # Load data
    print(f"Loading data from: {table_id}")
    df = load_weight_data_from_bigquery(config['project_id'], table_id)

    print(f"\nDataset Statistics:")
    print(f"  Total samples: {len(df)}")
    print(f"  Target weight range: {df['target'].min():.2f} - {df['target'].max():.2f} lbs")
    print(f"  Sample target texts: {df['target_text'].head().tolist()}")

    # Split data
    train_size = int(0.8 * len(df))
    val_size = int(0.1 * len(df))

    train_df = df.iloc[:train_size]
    val_df = df.iloc[train_size:train_size + val_size]
    test_df = df.iloc[train_size + val_size:]

    print(f"\nData splits:")
    print(f"  Training: {len(train_df)}")
    print(f"  Validation: {len(val_df)}")
    print(f"  Test: {len(test_df)}")

    # Initialize model and tokenizer
    print(f"\nLoading model: {args.model_name}")
    tokenizer = AutoTokenizer.from_pretrained(args.model_name)

    # Add special tokens
    special_tokens = {
        'additional_special_tokens': ['<|user|>', '<|assistant|>']
    }

    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    tokenizer.add_special_tokens(special_tokens)

    model = AutoModelForCausalLM.from_pretrained(
        args.model_name,
        torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
    )

    # Resize token embeddings for new special tokens
    model.resize_token_embeddings(len(tokenizer))

    # Enable gradient checkpointing
    model.gradient_checkpointing_enable()

    print(f"Model parameters: {sum(p.numel() for p in model.parameters()) / 1e6:.1f}M")

    # Create datasets
    train_dataset = WeightExtractionDataset(train_df, tokenizer, args.max_length)
    val_dataset = WeightExtractionDataset(val_df, tokenizer, args.max_length)
    test_dataset = WeightExtractionDataset(test_df, tokenizer, args.max_length)

    # Create dataloaders
    train_dataloader = DataLoader(
        train_dataset, batch_size=args.batch_size, shuffle=True, num_workers=0
    )
    val_dataloader = DataLoader(
        val_dataset, batch_size=args.eval_batch_size, shuffle=False, num_workers=0
    )
    test_dataloader = DataLoader(
        test_dataset, batch_size=args.eval_batch_size, shuffle=False, num_workers=0
    )

    # Training setup
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Training device: {device}")
    model.to(device)

    clear_gpu_memory()

    # Optimizer and scheduler
    optimizer = AdamW(model.parameters(), lr=args.lr, eps=1e-8)

    total_steps = len(train_dataloader) * args.epochs // args.gradient_accumulation_steps
    scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=args.warmup_steps,
        num_training_steps=total_steps
    )

    print(f"Total training steps: {total_steps}")

    # Training loop
    model.train()
    global_step = 0
    best_val_accuracy = 0.0

    for epoch in range(args.epochs):
        print(f"\n=== Epoch {epoch + 1}/{args.epochs} ===")
        epoch_loss = 0
        epoch_steps = 0
        accumulation_loss = 0

        progress_bar = tqdm(train_dataloader, desc=f"Epoch {epoch + 1}")

        for step, batch in enumerate(progress_bar):
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            labels = batch['labels'].to(device)

            outputs = model(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=labels
            )

            loss = outputs.loss / args.gradient_accumulation_steps
            accumulation_loss += loss.item()
            epoch_loss += loss.item()
            epoch_steps += 1

            loss.backward()

            if (step + 1) % args.gradient_accumulation_steps == 0:
                torch.nn.utils.clip_grad_norm_(model.parameters(), args.gradient_clip)
                optimizer.step()
                scheduler.step()
                optimizer.zero_grad()
                global_step += 1

                progress_bar.set_postfix({
                    'loss': f'{accumulation_loss:.4f}',
                    'avg_loss': f'{epoch_loss / epoch_steps:.4f}',
                    'lr': f'{scheduler.get_last_lr()[0]:.2e}',
                    'step': global_step
                })

                accumulation_loss = 0

                # Validation
                if global_step % args.eval_steps == 0:
                    print(f"\nValidation at step {global_step}")
                    val_metrics = evaluate_model(
                        model, val_dataloader, tokenizer, device,
                        max_batches=args.max_eval_batches
                    )

                    print(f"Val Loss: {val_metrics['loss']:.4f}")
                    print(f"Val Exact Match: {val_metrics['exact_match_accuracy']:.4f}")
                    print(f"Val Parse Success: {val_metrics['parse_success_rate']:.4f}")
                    print(f"Val Weight MAE: {val_metrics['weight_mae']:.4f}")

                    print("\nSample predictions vs targets:")
                    for i in range(min(3, len(val_metrics['predictions']))):
                        print(f"  Pred: '{val_metrics['predictions'][i]}'")
                        print(f"  True: '{val_metrics['true_targets'][i]}'")

                    if val_metrics['exact_match_accuracy'] > best_val_accuracy:
                        best_val_accuracy = val_metrics['exact_match_accuracy']
                        print(f"🎉 New best validation accuracy: {best_val_accuracy:.4f}")

                    model.train()

            del input_ids, attention_mask, labels, outputs

            if step % 25 == 0:
                clear_gpu_memory()

        avg_epoch_loss = epoch_loss / epoch_steps
        print(f"Epoch {epoch + 1} completed - Average Loss: {avg_epoch_loss:.4f}")
        clear_gpu_memory()

    # Final test evaluation
    print("\n" + "=" * 60)
    print("FINAL TEST EVALUATION")
    print("=" * 60)
    test_metrics = evaluate_model(
        model, test_dataloader, tokenizer, device,
        max_batches=args.max_eval_batches * 2
    )

    print(f"Test Loss: {test_metrics['loss']:.4f}")
    print(f"Test Exact Match Accuracy: {test_metrics['exact_match_accuracy']:.4f}")
    print(f"Test Parse Success Rate: {test_metrics['parse_success_rate']:.4f}")
    print(f"Test Weight MAE: {test_metrics['weight_mae']:.4f}")
    print(f"Test Weight RMSE: {test_metrics['weight_rmse']:.4f}")

    print(f"\nFinal sample predictions:")
    for i in range(min(5, len(test_metrics['predictions']))):
        print(f"  Prediction: '{test_metrics['predictions'][i]}'")
        print(f"  Ground Truth: '{test_metrics['true_targets'][i]}'")
        pred_weight = calculate_total_weight_from_prediction(test_metrics['predictions'][i])
        print(f"  Parsed Weight: {pred_weight} lbs")

    clear_gpu_memory()

    # Save model
    print("\nSaving model to GCS...")
    save_model_to_gcs(model, tokenizer, args.bucket_name, args.model_path)

    # Save config and results
    config_data = {
        'model_name': args.model_name,
        'task_type': 'weight_text_generation_decoder',
        'epochs': args.epochs,
        'batch_size': args.batch_size,
        'gradient_accumulation_steps': args.gradient_accumulation_steps,
        'learning_rate': args.lr,
        'max_length': args.max_length,
        'best_val_accuracy': best_val_accuracy,
        'test_metrics': test_metrics,
        'total_parameters': sum(p.numel() for p in model.parameters()),
        'trainable_parameters': sum(p.numel() for p in model.parameters() if p.requires_grad),
        'training_samples': len(train_df)
    }

    client = storage.Client()
    bucket = client.bucket(args.bucket_name)
    config_blob = bucket.blob(f"{args.model_path}/training_config.json")
    config_blob.upload_from_string(json.dumps(config_data, indent=2))

    print(f"✅ Decoder weight extraction model saved to gs://{args.bucket_name}/{args.model_path}")
    print("✅ Training completed successfully!")


if __name__ == "__main__":
    main()