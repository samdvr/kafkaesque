# Kafkaesque Infrastructure - Google Cloud Platform
#
# This Terraform module provisions the GCP infrastructure for Kafkaesque:
# - GCS bucket for message storage
# - Service account with GCS access
# - Workload Identity configuration for GKE
#
# Usage:
#   cd deploy/terraform/gcp
#   terraform init
#   terraform plan -var="project_id=my-project" -var="cluster_name=kafkaesque-prod"
#   terraform apply -var="project_id=my-project" -var="cluster_name=kafkaesque-prod"

terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Variables
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "cluster_name" {
  description = "Name of the Kafkaesque cluster"
  type        = string
  default     = "kafkaesque"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name for Kafkaesque data (leave empty to auto-generate)"
  type        = string
  default     = ""
}

variable "enable_versioning" {
  description = "Enable GCS versioning"
  type        = bool
  default     = true
}

variable "gke_cluster_name" {
  description = "GKE cluster name (for Workload Identity)"
  type        = string
  default     = ""
}

variable "gke_namespace" {
  description = "Kubernetes namespace for Kafkaesque"
  type        = string
  default     = "kafkaesque"
}

variable "gke_service_account" {
  description = "Kubernetes service account name for Kafkaesque"
  type        = string
  default     = "kafkaesque"
}

variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}

# Provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
}

# Local variables
locals {
  bucket_name = var.gcs_bucket_name != "" ? var.gcs_bucket_name : "${var.cluster_name}-${var.environment}-${var.project_id}"

  common_labels = merge(var.labels, {
    cluster     = var.cluster_name
    environment = var.environment
    managed-by  = "terraform"
    application = "kafkaesque"
  })
}

# GCS Bucket for Kafkaesque data
resource "google_storage_bucket" "kafkaesque_data" {
  name          = local.bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = var.enable_versioning
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = local.common_labels
}

# Service Account for Kafkaesque
resource "google_service_account" "kafkaesque" {
  account_id   = "${var.cluster_name}-${var.environment}"
  display_name = "Kafkaesque Service Account - ${var.cluster_name}"
  description  = "Service account for Kafkaesque message broker"
  project      = var.project_id
}

# IAM binding for GCS access
resource "google_storage_bucket_iam_member" "kafkaesque_data_admin" {
  bucket = google_storage_bucket.kafkaesque_data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.kafkaesque.email}"
}

# Workload Identity binding (if GKE cluster specified)
resource "google_service_account_iam_member" "workload_identity" {
  count = var.gke_cluster_name != "" ? 1 : 0

  service_account_id = google_service_account.kafkaesque.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.gke_namespace}/${var.gke_service_account}]"
}

# Outputs
output "gcs_bucket_name" {
  description = "GCS bucket name for Kafkaesque data"
  value       = google_storage_bucket.kafkaesque_data.name
}

output "gcs_bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.kafkaesque_data.url
}

output "service_account_email" {
  description = "Service account email"
  value       = google_service_account.kafkaesque.email
}

output "helm_values" {
  description = "Helm values for Kafkaesque deployment"
  value = <<-EOT
# Add these values to your Kafkaesque Helm deployment
config:
  objectStore:
    type: gcs
    gcs:
      bucket: ${google_storage_bucket.kafkaesque_data.name}

# If using Workload Identity:
# serviceAccount:
#   annotations:
#     iam.gke.io/gcp-service-account: ${google_service_account.kafkaesque.email}
EOT
}
