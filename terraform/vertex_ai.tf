# Enable Vertex AI API
resource "google_project_service" "vertex_ai" {
  service = "aiplatform.googleapis.com"
}

# Enable BigQuery API
resource "google_project_service" "bigquery_api" {
  service = "bigquery.googleapis.com"
}

# Service account for Vertex AI training
resource "google_service_account" "vertex_training_sa" {
  account_id   = "vertex-training-sa"
  display_name = "Vertex AI Training Service Account"
}

# IAM permissions for training job - BigQuery
resource "google_project_iam_member" "vertex_bigquery_data" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

resource "google_project_iam_member" "vertex_bigquery_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

resource "google_project_iam_member" "vertex_bigquery_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

# IAM permissions for Cloud Storage
resource "google_project_iam_member" "vertex_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

# IAM permissions for Vertex AI
resource "google_project_iam_member" "vertex_ai_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

# Artifact Registry for training images
resource "google_artifact_registry_repository" "ml_repo" {
  location      = "us-central1"
  repository_id = "ml-training"
  description   = "ML training container repository"
  format        = "DOCKER"
}

# Cloud Storage bucket for model outputs
resource "google_storage_bucket" "model_artifacts" {
  name     = "${var.project_id}-model-artifacts"
  location = "US"
  
  versioning {
    enabled = true
  }
}