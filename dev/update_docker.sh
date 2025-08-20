#!/bin/bash

# Simple script to rebuild and push Docker image

IMAGE_NAME="us-central1-docker.pkg.dev/mlops-466819/ml-training/encoder-trainer:latest"

echo "Building Docker image..."
docker build -t $IMAGE_NAME .

echo "Pushing to Artifact Registry..."
docker push $IMAGE_NAME

echo "Done!"
