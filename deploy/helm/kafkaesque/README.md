# Kafkaesque Helm Chart

A Helm chart for deploying Kafkaesque, a Kafka-compatible message broker using object storage.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.0+
- PV provisioner support (for persistence)

## Installation

```bash
# Add the repository (if published)
# helm repo add kafkaesque https://your-repo.example.com

# Install with default values
helm install kafkaesque ./kafkaesque -n kafkaesque --create-namespace

# Install with custom values
helm install kafkaesque ./kafkaesque -n kafkaesque --create-namespace -f my-values.yaml
```

## Configuration

See [values.yaml](values.yaml) for the full list of configurable parameters.

### Common Configuration

```yaml
# Scale to 5 replicas
replicaCount: 5

# Use S3 storage
config:
  objectStore:
    type: s3
    s3:
      bucket: my-kafkaesque-bucket
      region: us-east-1

# Enable Prometheus monitoring
serviceMonitor:
  enabled: true
```

### Using S3 Storage

```bash
# Create secret for S3 credentials
kubectl create secret generic kafkaesque-s3-credentials -n kafkaesque \
  --from-literal=AWS_ACCESS_KEY_ID=your-key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your-secret

# Install with S3 config
helm install kafkaesque ./kafkaesque -n kafkaesque --create-namespace \
  --set config.objectStore.type=s3 \
  --set config.objectStore.s3.bucket=my-bucket \
  --set config.objectStore.s3.region=us-east-1 \
  --set extraEnvFrom[0].secretRef.name=kafkaesque-s3-credentials
```

### Production Configuration

```yaml
# production-values.yaml
replicaCount: 5

resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"

persistence:
  size: 100Gi
  storageClass: fast-ssd

podAntiAffinity:
  enabled: true
  type: hard

podDisruptionBudget:
  enabled: true
  minAvailable: 3

serviceMonitor:
  enabled: true
```

## Health Endpoints

The chart configures the following health probes:

- **Liveness**: `/health` - Returns 200 if server is running
- **Readiness**: `/ready` - Returns 503 if in zombie mode
- **Startup**: `/health` - Allows 30 attempts before marking pod unhealthy

## Upgrading

```bash
helm upgrade kafkaesque ./kafkaesque -n kafkaesque
```

## Uninstalling

```bash
helm uninstall kafkaesque -n kafkaesque

# If you want to delete PVCs (data loss!)
kubectl delete pvc -l app.kubernetes.io/name=kafkaesque -n kafkaesque
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| replicaCount | int | `3` | Number of Kafkaesque brokers |
| image.repository | string | `"kafkaesque"` | Image repository |
| image.tag | string | `"latest"` | Image tag |
| config.clusterId | string | `"kafkaesque-cluster"` | Cluster identifier |
| config.objectStore.type | string | `"local"` | Storage type: local, s3, gcs, azure |
| persistence.enabled | bool | `true` | Enable persistent storage |
| persistence.size | string | `"10Gi"` | Storage size per broker |
| podDisruptionBudget.enabled | bool | `true` | Enable PDB |
| serviceMonitor.enabled | bool | `false` | Enable Prometheus ServiceMonitor |

See [values.yaml](values.yaml) for all options.
