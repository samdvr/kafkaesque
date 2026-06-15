# Kafkaesque Infrastructure - Azure
#
# This Terraform module provisions the Azure infrastructure for Kafkaesque:
# - Storage account and container for message storage
# - Managed Identity for blob access
# - AKS pod identity configuration
#
# Usage:
#   cd deploy/terraform/azure
#   terraform init
#   terraform plan -var="cluster_name=kafkaesque-prod" -var="resource_group_name=kafkaesque-rg"
#   terraform apply -var="cluster_name=kafkaesque-prod" -var="resource_group_name=kafkaesque-rg"

terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
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

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "create_resource_group" {
  description = "Whether to create the resource group"
  type        = bool
  default     = true
}

variable "storage_account_name" {
  description = "Storage account name (leave empty to auto-generate)"
  type        = string
  default     = ""
}

variable "container_name" {
  description = "Blob container name"
  type        = string
  default     = "kafkaesque-data"
}

variable "aks_cluster_name" {
  description = "AKS cluster name (for pod identity)"
  type        = string
  default     = ""
}

variable "aks_namespace" {
  description = "Kubernetes namespace for Kafkaesque"
  type        = string
  default     = "kafkaesque"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

# Provider configuration
provider "azurerm" {
  features {}
}

# Data sources
data "azurerm_client_config" "current" {}

# Local variables
locals {
  # Storage account names must be lowercase, alphanumeric, 3-24 chars
  storage_account_name = var.storage_account_name != "" ? var.storage_account_name : lower(replace("${var.cluster_name}${var.environment}${substr(data.azurerm_client_config.current.subscription_id, 0, 8)}", "-", ""))

  common_tags = merge(var.tags, {
    Cluster     = var.cluster_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Application = "kafkaesque"
  })
}

# Resource Group
resource "azurerm_resource_group" "kafkaesque" {
  count    = var.create_resource_group ? 1 : 0
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

data "azurerm_resource_group" "kafkaesque" {
  count = var.create_resource_group ? 0 : 1
  name  = var.resource_group_name
}

locals {
  resource_group_name = var.create_resource_group ? azurerm_resource_group.kafkaesque[0].name : data.azurerm_resource_group.kafkaesque[0].name
  resource_group_id   = var.create_resource_group ? azurerm_resource_group.kafkaesque[0].id : data.azurerm_resource_group.kafkaesque[0].id
}

# Storage Account
resource "azurerm_storage_account" "kafkaesque" {
  name                     = substr(local.storage_account_name, 0, 24)
  resource_group_name      = local.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  blob_properties {
    versioning_enabled = true

    delete_retention_policy {
      days = 7
    }

    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.common_tags
}

# Blob Container
resource "azurerm_storage_container" "kafkaesque_data" {
  name                  = var.container_name
  storage_account_name  = azurerm_storage_account.kafkaesque.name
  container_access_type = "private"
}

# User Assigned Identity for Kafkaesque
resource "azurerm_user_assigned_identity" "kafkaesque" {
  name                = "${var.cluster_name}-${var.environment}-identity"
  resource_group_name = local.resource_group_name
  location            = var.location
  tags                = local.common_tags
}

# Role assignment for blob access
resource "azurerm_role_assignment" "kafkaesque_blob_contributor" {
  scope                = azurerm_storage_account.kafkaesque.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.kafkaesque.principal_id
}

# Outputs
output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.kafkaesque.name
}

output "container_name" {
  description = "Blob container name"
  value       = azurerm_storage_container.kafkaesque_data.name
}

output "storage_account_primary_access_key" {
  description = "Storage account access key"
  value       = azurerm_storage_account.kafkaesque.primary_access_key
  sensitive   = true
}

output "managed_identity_client_id" {
  description = "Managed identity client ID"
  value       = azurerm_user_assigned_identity.kafkaesque.client_id
}

output "managed_identity_id" {
  description = "Managed identity resource ID"
  value       = azurerm_user_assigned_identity.kafkaesque.id
}

output "helm_values" {
  description = "Helm values for Kafkaesque deployment"
  value = <<-EOT
# Add these values to your Kafkaesque Helm deployment
config:
  objectStore:
    type: azure
    azure:
      container: ${azurerm_storage_container.kafkaesque_data.name}
      account: ${azurerm_storage_account.kafkaesque.name}

# For access key authentication (store key in secret):
# extraEnvFrom:
#   - secretRef:
#       name: kafkaesque-azure-credentials

# For pod identity authentication:
# podLabels:
#   aadpodidbinding: ${var.cluster_name}-${var.environment}-identity
EOT
}
