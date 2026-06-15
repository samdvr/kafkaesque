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
kubectl apply -f secret.yaml
kubectl apply -f service.yaml
kubectl apply -f statefulset.yaml
kubectl apply -f pdb.yaml
```

**Before production use:** replace the placeholder values in `secret.yaml` with strong random secrets (see comments in that file), or create the secret out-of-band and remove `secret.yaml` from `kustomization.yaml`.

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

The default ConfigMap enables ACL enforcement with `User:ANONYMOUS` as a bootstrap super-user so unauthenticated clients work on an internal network. **Remove this before exposing the cluster outside a trusted mesh** and configure SASL + per-principal ACLs instead.

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
| `RAFT_CLUSTER_SECRET` | (required) | HMAC key for steady-state Raft RPCs |
| `RAFT_JOIN_TOKEN` | (required) | HMAC key for new-node joins |
| `ACL_ENABLED` | `true` | Enable ACL enforcement |
| `OBJECT_STORE_TYPE` | `local` | `local`, `s3`, `gcs`, or `azure` |
| `DATA_PATH` | `/data/kafkaesque` | Local storage path |

### Storage and High Availability

**Critical:** `OBJECT_STORE_TYPE=local` uses a per-pod persistent volume. Partition data is **not** shared across brokers. The default manifest runs **one broker** for this reason.

To run a multi-broker HA cluster:

1. Switch to shared object storage (S3, GCS, or Azure) in the ConfigMap.
2. Scale the StatefulSet to 3+ replicas.
3. Update `RAFT_PEERS` in the StatefulSet to list every broker.
4. Increase `podDisruptionBudget.minAvailable` accordingly.

The broker refuses to start if `OBJECT_STORE_TYPE=local` and `RAFT_PEERS` lists more than one member.

### Using S3 Storage

For production HA, use S3 (or S3-compatible) storage:

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

Then patch the StatefulSet to mount credentials and scale to 3 replicas with updated `RAFT_PEERS`.

## Scaling

```bash
# Only after switching to shared object storage (S3/GCS/Azure):
kubectl scale statefulset kafkaesque -n kafkaesque --replicas=3
```

When scaling, update `RAFT_PEERS` to include all broker addresses.

## Cleanup

```bash
kubectl delete -k .
# Or delete namespace (removes everything)
kubectl delete namespace kafkaesque
```
