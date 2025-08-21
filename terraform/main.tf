terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "us-central1"
  zone    = "us-central1-b"
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "mlops-466819"
}

# Get project data for service account reference
data "google_project" "project" {
  project_id = var.project_id
}

# ==================== ENABLE APIs ====================
resource "google_project_service" "vertex_ai" {
  service = "aiplatform.googleapis.com"
}

resource "google_project_service" "bigquery_api" {
  service = "bigquery.googleapis.com"
}

resource "google_project_service" "storage_api" {
  service = "storage.googleapis.com"
}

resource "google_project_service" "artifact_registry_api" {
  service = "artifactregistry.googleapis.com"
}

# ==================== NETWORKING ====================
resource "google_compute_network" "vpc_network" {
  name = "mlops-network"
}

# ==================== SERVICE ACCOUNTS ====================
resource "google_service_account" "vertex_training_sa" {
  account_id   = "vertex-training-sa"
  display_name = "Vertex AI Training Service Account"
}

# ==================== BIGQUERY ====================
resource "google_bigquery_dataset" "gfv_dataset" {
  dataset_id    = "gfv_dataset"
  friendly_name = "Grass Fed Valley Dataset"
  description   = "Dataset for Grass Fed Valley"
  location      = "US"

  labels = {
    env     = "dev"
    project = "mlops"
  }

  access {
    role          = "OWNER"
    user_by_email = "brandt.333@hotmail.com"
  }

  access {
    role   = "READER"
    user_by_email = "service-${data.google_project.project.number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
  }

  access {
    role   = "READER"
    user_by_email = google_service_account.vertex_training_sa.email
  }
}

resource "google_bigquery_table" "gfv_table" {
  dataset_id = google_bigquery_dataset.gfv_dataset.dataset_id
  table_id   = "gfv_data"

  deletion_protection = false

  labels = {
    env     = "dev"
    project = "mlops"
  }

  schema = jsonencode([
    {
      name        = "name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product name"
    },
    {
      name        = "description"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product description"
    },
    {
      name        = "variant_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product variant name"
    },
    {
      name        = "url"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product URL"
    },
    {
      name        = "target"
      type        = "FLOAT"
      mode        = "NULLABLE"
      description = "Weight in pounds (lbs) - target variable for ML"
    }
  ])
}

resource "google_bigquery_table" "gfv_training_table" {
  dataset_id = google_bigquery_dataset.gfv_dataset.dataset_id
  table_id   = "gfv_data_hashed_training"

  deletion_protection = false

  labels = {
    env     = "dev"
    project = "mlops"
    type    = "training"
  }

  schema = jsonencode([
    {
      name        = "name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product name"
    },
    {
      name        = "variant"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product variant name"
    },
    {
      name        = "description"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Product description"
    },
    {
      name        = "weight"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Original weight raw text"
    },
    {
      name        = "extracted_weight"
      type        = "FLOAT"
      mode        = "NULLABLE"
      description = "Individual weight extracted (lbs)"
    },
    {
      name        = "extracted_quantity"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Quantity extracted"
    },
    {
      name        = "extraction_method"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Method used for weight extraction"
    },
    {
      name        = "target"
      type        = "FLOAT"
      mode        = "NULLABLE"
      description = "Final calculated weight (lbs) - target variable for ML"
    },
    {
      name        = "target_text"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Target in text format for text-to-text models"
    }
  ])
}

# ==================== STORAGE ====================
resource "google_storage_bucket" "model_artifacts" {
  name     = "${var.project_id}-model-artifacts"
  location = "US"

  versioning {
    enabled = true
  }

  labels = {
    env     = "dev"
    project = "mlops"
  }
}

# ==================== ARTIFACT REGISTRY ====================
resource "google_artifact_registry_repository" "ml_repo" {
  location      = "us-central1"
  repository_id = "ml-training"
  description   = "ML training container repository"
  format        = "DOCKER"

  labels = {
    env     = "dev"
    project = "mlops"
  }
}

# ==================== IAM PERMISSIONS ====================
# BigQuery permissions for custom service account
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

# Storage permissions for custom service account
resource "google_project_iam_member" "vertex_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

# Vertex AI permissions for custom service account
resource "google_project_iam_member" "vertex_ai_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.vertex_training_sa.email}"
}

# BigQuery permissions for default Vertex AI service account
resource "google_project_iam_member" "vertex_default_sa_bigquery_data" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "vertex_default_sa_bigquery_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "vertex_default_sa_bigquery_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
}

# Storage permissions for default Vertex AI service account
resource "google_project_iam_member" "vertex_default_sa_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
}

# ==================== OUTPUTS ====================
output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "project_number" {
  description = "GCP Project Number"
  value       = data.google_project.project.number
}

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.gfv_dataset.dataset_id
}

output "table_id" {
  description = "BigQuery table ID"
  value       = google_bigquery_table.gfv_table.table_id
}

output "training_table_id" {
  description = "BigQuery training table ID"
  value       = google_bigquery_table.gfv_training_table.table_id
}

output "full_table_id" {
  description = "Full BigQuery table ID for queries"
  value       = "${var.project_id}.${google_bigquery_dataset.gfv_dataset.dataset_id}.${google_bigquery_table.gfv_table.table_id}"
}

output "full_training_table_id" {
  description = "Full BigQuery training table ID for queries"
  value       = "${var.project_id}.${google_bigquery_dataset.gfv_dataset.dataset_id}.${google_bigquery_table.gfv_training_table.table_id}"
}

output "dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.gfv_dataset.location
}

output "bucket_name" {
  description = "Cloud Storage bucket name"
  value       = google_storage_bucket.model_artifacts.name
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository URL"
  value       = "${google_artifact_registry_repository.ml_repo.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ml_repo.repository_id}"
}

output "vertex_training_service_account" {
  description = "Vertex AI training service account email"
  value       = google_service_account.vertex_training_sa.email
}