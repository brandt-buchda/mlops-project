import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from transformers import AutoModel, AutoTokenizer
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import argparse
import os
import json


class ProductDataset(Dataset):
    def __init__(self, texts, targets, tokenizer, max_length=512):
        self.texts = texts
        self.targets = targets
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = self.texts[idx]
        target = self.targets[idx]

        encoding = self.tokenizer(
            text,
            truncation=True,
            padding='max_length',
            max_length=self.max_length,
            return_tensors='pt'
        )

        return {
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'target': torch.tensor(target, dtype=torch.float32)
        }


class EncoderModel(nn.Module):
    def __init__(self, model_name):
        super().__init__()
        self.transformer = AutoModel.from_pretrained(model_name)
        self.regression_head = nn.Linear(self.transformer.config.hidden_size, 1)

    def forward(self, input_ids, attention_mask):
        outputs = self.transformer(input_ids=input_ids, attention_mask=attention_mask)
        pooled = outputs.last_hidden_state[:, 0]
        return self.regression_head(pooled).squeeze()


def load_data_from_bigquery(project_id, table_id):
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT name, description, variant_name, target 
        FROM `{table_id}` 
        WHERE target IS NOT NULL
    """

    df = client.query(query).to_dataframe()
    texts = [f"{row['name']} {row['description']} {row['variant_name']}" for _, row in df.iterrows()]
    targets = df['target'].tolist()

    return texts, targets


def save_model_to_gcs(model, tokenizer, bucket_name, model_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Save model
    torch.save(model.state_dict(), '/tmp/model.pt')
    blob = bucket.blob(f"{model_path}/model.pt")
    blob.upload_from_filename('/tmp/model.pt')

    # Save tokenizer
    tokenizer.save_pretrained('/tmp/tokenizer')
    for file in os.listdir('/tmp/tokenizer'):
        blob = bucket.blob(f"{model_path}/tokenizer/{file}")
        blob.upload_from_filename(f'/tmp/tokenizer/{file}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--table-id', required=True)
    parser.add_argument('--model-name', default='sentence-transformers/all-MiniLM-L6-v2')
    parser.add_argument('--bucket-name', required=True)
    parser.add_argument('--model-path', default='models/encoder')
    parser.add_argument('--epochs', type=int, default=5)
    parser.add_argument('--batch-size', type=int, default=16)
    parser.add_argument('--lr', type=float, default=2e-4)

    args = parser.parse_args()

    # Comprehensive GPU diagnostics
    print("=" * 60)
    print("VERTEX AI GPU DIAGNOSTICS")
    print("=" * 60)
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"CUDA version: {torch.version.cuda}")
        print(f"cuDNN version: {torch.backends.cudnn.version()}")
        print(f"GPU count: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            gpu_props = torch.cuda.get_device_properties(i)
            print(f"GPU {i}: {gpu_props.name}")
            print(f"  Memory: {gpu_props.total_memory / 1e9:.1f} GB")
            print(f"  Compute Capability: {gpu_props.major}.{gpu_props.minor}")
        print(f"Current GPU: {torch.cuda.current_device()}")

        # Test GPU allocation
        test_tensor = torch.randn(10, 10).cuda()
        print(f"Test tensor device: {test_tensor.device}")
        print("✅ GPU allocation successful!")
    else:
        print("❌ NO GPU DETECTED - Using CPU")
    print("=" * 60)

    # Load data
    texts, targets = load_data_from_bigquery(args.project_id, args.table_id)
    print(f"Loaded {len(texts)} training samples")

    # Initialize tokenizer and model
    tokenizer = AutoTokenizer.from_pretrained(args.model_name)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = EncoderModel(args.model_name)

    # Create dataset and dataloader
    dataset = ProductDataset(texts, targets, tokenizer)
    dataloader = DataLoader(dataset, batch_size=args.batch_size, shuffle=True, num_workers=2)

    # Training setup
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Training device: {device}")

    model.to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr)
    criterion = nn.MSELoss()

    # Training loop
    model.train()
    for epoch in range(args.epochs):
        total_loss = 0
        batch_count = 0

        for batch in dataloader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)
            targets = batch['target'].to(device)

            # Log first batch device info
            if epoch == 0 and batch_count == 0:
                print(f"First batch tensor devices: {input_ids.device}")

            optimizer.zero_grad()
            outputs = model(input_ids, attention_mask)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
            batch_count += 1

        avg_loss = total_loss / len(dataloader)
        print(f"Epoch {epoch + 1}/{args.epochs}, Average Loss: {avg_loss:.4f}")

    # Save model to GCS
    save_model_to_gcs(model, tokenizer, args.bucket_name, args.model_path)
    print(f"✅ Model saved to gs://{args.bucket_name}/{args.model_path}")


if __name__ == "__main__":
    main()