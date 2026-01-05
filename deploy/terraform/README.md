# Kafkaesque Terraform Modules

This directory contains Terraform modules for provisioning cloud infrastructure for Kafkaesque.

## Available Modules

### AWS (`./aws`)

Provisions:
- S3 bucket for message storage
- IAM policy for S3 access
- IAM role with IRSA (IAM Roles for Service Accounts) for EKS

```bash
cd aws
terraform init
terraform apply -var="cluster_name=kafkaesque-prod" -var="eks_cluster_name=my-eks-cluster"
```

### GCP (`./gcp`)

Provisions:
- GCS bucket for message storage
- Service account with storage access
- Workload Identity binding for GKE

```bash
cd gcp
terraform init
terraform apply -var="project_id=my-project" -var="cluster_name=kafkaesque-prod"
```

### Azure (`./azure`)

Provisions:
- Storage account and blob container
- User-assigned managed identity
- Role assignment for blob access

```bash
cd azure
terraform init
terraform apply -var="cluster_name=kafkaesque-prod" -var="resource_group_name=kafkaesque-rg"
```

## Integration with Helm

After running Terraform, use the output `helm_values` to configure your Kafkaesque deployment:

```bash
# Get Helm values from Terraform output
terraform output -raw helm_values > kafkaesque-cloud-values.yaml

# Install Kafkaesque with cloud storage
helm install kafkaesque ../helm/kafkaesque -n kafkaesque --create-namespace -f kafkaesque-cloud-values.yaml
```

## State Management

For production, configure remote state:

```hcl
# backend.tf
terraform {
  backend "s3" {
    bucket = "my-terraform-state"
    key    = "kafkaesque/terraform.tfstate"
    region = "us-east-1"
  }
}
```

## Security Best Practices

1. **Use IAM Roles/Managed Identities**: Avoid storing access keys in Kubernetes secrets
2. **Enable Encryption**: All modules enable server-side encryption by default
3. **Block Public Access**: All storage is configured as private
4. **Enable Versioning**: Protect against accidental data loss
5. **Use Lifecycle Policies**: Automatically transition data to cheaper storage tiers
