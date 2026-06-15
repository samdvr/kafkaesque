# Kafkaesque Infrastructure - AWS
#
# This Terraform module provisions the AWS infrastructure for Kafkaesque:
# - S3 bucket for message storage
# - IAM role and policy for S3 access
# - Security groups
# - Optional: EKS cluster configuration
#
# Usage:
#   cd deploy/terraform/aws
#   terraform init
#   terraform plan -var="cluster_name=kafkaesque-prod"
#   terraform apply -var="cluster_name=kafkaesque-prod"

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Variables
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

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for Kafkaesque data (leave empty to auto-generate)"
  type        = string
  default     = ""
}

variable "enable_versioning" {
  description = "Enable S3 versioning"
  type        = bool
  default     = true
}

variable "eks_cluster_name" {
  description = "EKS cluster name (for IRSA configuration)"
  type        = string
  default     = ""
}

variable "eks_namespace" {
  description = "Kubernetes namespace for Kafkaesque"
  type        = string
  default     = "kafkaesque"
}

variable "eks_service_account" {
  description = "Kubernetes service account name for Kafkaesque"
  type        = string
  default     = "kafkaesque"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

# Provider configuration
provider "aws" {
  region = var.aws_region
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Local variables
locals {
  bucket_name = var.s3_bucket_name != "" ? var.s3_bucket_name : "${var.cluster_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"

  common_tags = merge(var.tags, {
    Cluster     = var.cluster_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Application = "kafkaesque"
  })
}

# S3 Bucket for Kafkaesque data
resource "aws_s3_bucket" "kafkaesque_data" {
  bucket = local.bucket_name

  tags = merge(local.common_tags, {
    Name = "${var.cluster_name}-data"
  })
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "kafkaesque_data" {
  bucket = aws_s3_bucket.kafkaesque_data.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# S3 Bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "kafkaesque_data" {
  bucket = aws_s3_bucket.kafkaesque_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 Bucket public access block
resource "aws_s3_bucket_public_access_block" "kafkaesque_data" {
  bucket = aws_s3_bucket.kafkaesque_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket lifecycle rules
resource "aws_s3_bucket_lifecycle_configuration" "kafkaesque_data" {
  bucket = aws_s3_bucket.kafkaesque_data.id

  rule {
    id     = "cleanup-incomplete-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# IAM Policy for Kafkaesque S3 access
resource "aws_iam_policy" "kafkaesque_s3" {
  name        = "${var.cluster_name}-${var.environment}-s3-access"
  description = "IAM policy for Kafkaesque to access S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.kafkaesque_data.arn,
          "${aws_s3_bucket.kafkaesque_data.arn}/*"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# IAM Role for Kafkaesque (IRSA - IAM Roles for Service Accounts)
resource "aws_iam_role" "kafkaesque" {
  count = var.eks_cluster_name != "" ? 1 : 0

  name = "${var.cluster_name}-${var.environment}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${replace(data.aws_eks_cluster.cluster[0].identity[0].oidc[0].issuer, "https://", "")}"
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${replace(data.aws_eks_cluster.cluster[0].identity[0].oidc[0].issuer, "https://", "")}:sub" = "system:serviceaccount:${var.eks_namespace}:${var.eks_service_account}"
          }
        }
      }
    ]
  })

  tags = local.common_tags
}

# Attach S3 policy to the IAM role
resource "aws_iam_role_policy_attachment" "kafkaesque_s3" {
  count = var.eks_cluster_name != "" ? 1 : 0

  role       = aws_iam_role.kafkaesque[0].name
  policy_arn = aws_iam_policy.kafkaesque_s3.arn
}

# EKS cluster data (if specified)
data "aws_eks_cluster" "cluster" {
  count = var.eks_cluster_name != "" ? 1 : 0
  name  = var.eks_cluster_name
}

# Outputs
output "s3_bucket_name" {
  description = "S3 bucket name for Kafkaesque data"
  value       = aws_s3_bucket.kafkaesque_data.bucket
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.kafkaesque_data.arn
}

output "s3_bucket_region" {
  description = "S3 bucket region"
  value       = data.aws_region.current.name
}

output "iam_policy_arn" {
  description = "IAM policy ARN for S3 access"
  value       = aws_iam_policy.kafkaesque_s3.arn
}

output "iam_role_arn" {
  description = "IAM role ARN (for IRSA)"
  value       = var.eks_cluster_name != "" ? aws_iam_role.kafkaesque[0].arn : null
}

output "helm_values" {
  description = "Helm values for Kafkaesque deployment"
  value = <<-EOT
# Add these values to your Kafkaesque Helm deployment
config:
  objectStore:
    type: s3
    s3:
      bucket: ${aws_s3_bucket.kafkaesque_data.bucket}
      region: ${data.aws_region.current.name}

# If using IRSA:
# serviceAccount:
#   annotations:
#     eks.amazonaws.com/role-arn: ${var.eks_cluster_name != "" ? aws_iam_role.kafkaesque[0].arn : "ROLE_ARN_HERE"}
EOT
}
