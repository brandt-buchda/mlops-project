terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}


provider "google" {
  project = "mlops-466819"
  region  = "us-central1"
  zone    = "us-central1-b"
}

resource "google_compute_network" "vpc_network" {
  name = "mlops-network"
}

# BigQuery Dataset
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
    user_by_email = "brandt.333@hotmail.com" # Replace with your email
  }
}

# BigQuery Table with schema (original)
resource "google_bigquery_table" "gfv_table" {
  dataset_id = google_bigquery_dataset.gfv_dataset.dataset_id
  table_id   = "gfv_data"

  deletion_protection = false # Set to true for production

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

# BigQuery Training Table with enhanced schema
resource "google_bigquery_table" "gfv_training_table" {
  dataset_id = google_bigquery_dataset.gfv_dataset.dataset_id
  table_id   = "gfv_data_hashed_training"

  deletion_protection = false # Set to true for production

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

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "mlops-466819"
}

# Outputs
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