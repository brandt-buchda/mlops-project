#!/bin/bash

# Set your variables (from your config.yml)
PROJECT_ID="mlops-466819"
REGION="us-central1"
IMAGE_NAME="t5-weight-trainer"

# Build and push the container
docker build -t $REGION-docker.pkg.dev/$PROJECT_ID/ml-training/$IMAGE_NAME:latest .
docker push $REGION-docker.pkg.dev/$PROJECT_ID/ml-training/$IMAGE_NAME:latest

## Simple script to rebuild and push Docker image
#
#IMAGE_NAME="us-central1-docker.pkg.dev/mlops-466819/ml-training/encoder-trainer:latest"
#
#echo "Building Docker image..."
#docker build -t $IMAGE_NAME .
#
#echo "Pushing to Artifact Registry..."
#docker push $IMAGE_NAME
#
#echo "Done!"
