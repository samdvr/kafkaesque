# Kafkaesque Kubernetes Deployment

This directory contains Kubernetes manifests for deploying Kafkaesque, a Kafka-compatible message broker.

## Prerequisites

- Kubernetes cluster (1.25+)
- kubectl configured
- Storage class for persistent volumes (optional but recommended)

## Quick Start

```bash
# Apply all manifests
kubectl apply -k .

# Or apply individually
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f service.yaml
kubectl apply -f statefulset.yaml
kubectl apply -f pdb.yaml
```

## Verify Deployment

```bash
# Check pod status
kubectl get pods -n kafkaesque

# Check service endpoints
kubectl get svc -n kafkaesque

# Check health endpoints
kubectl exec -n kafkaesque kafkaesque-0 -- curl -s http://localhost:8080/health
kubectl exec -n kafkaesque kafkaesque-0 -- curl -s http://localhost:8080/ready
kubectl exec -n kafkaesque kafkaesque-0 -- curl -s http://localhost:8080/metrics
```

## Connect to Kafkaesque

```bash
# Port forward for local access
kubectl port-forward -n kafkaesque svc/kafkaesque 9092:9092

# Then use any Kafka client
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

## Configuration

### Environment Variables

Override configuration via ConfigMap or environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_ID` | Pod ordinal | Unique broker identifier |
| `HOST` | `0.0.0.0` | Listen address |
| `PORT` | `9092` | Kafka protocol port |
| `HEALTH_PORT` | `8080` | Health check HTTP port |
| `RAFT_LISTEN_ADDR` | `0.0.0.0:9093` | Raft consensus port |
| `RAFT_PEERS` | Auto-configured | Cluster peer addresses |
| `OBJECT_STORE_TYPE` | `local` | `local`, `s3`, `gcs`, or `azure` |
| `DATA_PATH` | `/data/kafkaesque` | Local storage path |

### Using S3 Storage

For production, use S3 (or S3-compatible) storage:

```yaml
# Add to ConfigMap
data:
  OBJECT_STORE_TYPE: "s3"
  AWS_S3_BUCKET: "your-bucket"
  AWS_REGION: "us-east-1"

# Add secrets for credentials
kubectl create secret generic kafkaesque-s3-credentials -n kafkaesque \
  --from-literal=AWS_ACCESS_KEY_ID=your-key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your-secret
```

## Scaling

```bash
# Scale to 5 brokers
kubectl scale statefulset kafkaesque -n kafkaesque --replicas=5
```

Note: When scaling, update `RAFT_PEERS` to include all broker addresses.

## Cleanup

```bash
kubectl delete -k .
# Or delete namespace (removes everything)
kubectl delete namespace kafkaesque
```
