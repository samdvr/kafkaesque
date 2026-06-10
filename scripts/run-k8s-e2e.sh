#!/bin/bash
# Kubernetes StatefulSet E2E verification (audit P1-15).
#
# Boots a 3-broker StatefulSet on a local `kind` cluster, deploys a
# producer sidecar, and verifies that the broker addresses returned in
# Metadata are actually reachable. Without this, ADVERTISED_HOST
# regressions only surface in production K8s, not in CI.
#
# Requirements:
# - kind  >= 0.20
# - kubectl
# - docker (for kind to spin up its container)
#
# Usage:
#   KAFKAESQUE_IMAGE=kafkaesque:ci bash scripts/run-k8s-e2e.sh

set -euo pipefail

CLUSTER_NAME="kafkaesque-e2e"
NAMESPACE="kafkaesque"
IMAGE="${KAFKAESQUE_IMAGE:-kafkaesque:ci}"

cleanup() {
    echo "Cleaning up kind cluster..."
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Creating kind cluster '$CLUSTER_NAME'..."
kind create cluster --name "$CLUSTER_NAME" --wait 60s

echo "Loading image '$IMAGE' into kind..."
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

echo "Creating namespace and applying manifests..."
kubectl create namespace "$NAMESPACE"
kubectl -n "$NAMESPACE" apply -k deploy/kubernetes/

echo "Waiting for StatefulSet to be ready..."
kubectl -n "$NAMESPACE" rollout status statefulset/kafkaesque --timeout=300s

echo "Deploying kcat producer sidecar..."
kubectl -n "$NAMESPACE" run kcat-producer \
    --image=edenhill/kcat:1.7.1 \
    --restart=Never \
    --command -- sleep 600

kubectl -n "$NAMESPACE" wait --for=condition=ready pod/kcat-producer --timeout=60s

echo "Querying Metadata via headless service..."
METADATA=$(kubectl -n "$NAMESPACE" exec kcat-producer -- \
    kcat -L -b kafkaesque-headless:9092 -X api.version.request=false \
    -X broker.version.fallback=2.0.0)

echo "$METADATA"

# Audit P1-15 verification: every advertised broker address should be
# reachable from inside the cluster. We grep the Metadata output for
# the StatefulSet pod FQDNs (set via ADVERTISED_HOST in the manifest)
# and probe each one.
for i in 0 1 2; do
    EXPECTED="kafkaesque-${i}.kafkaesque-headless.${NAMESPACE}.svc.cluster.local"
    if ! echo "$METADATA" | grep -q "$EXPECTED"; then
        echo "FAIL: broker $i not advertising as $EXPECTED in Metadata"
        exit 1
    fi
    echo "Probing $EXPECTED:9092..."
    kubectl -n "$NAMESPACE" exec kcat-producer -- \
        kcat -L -b "$EXPECTED:9092" -X api.version.request=false \
        -X broker.version.fallback=2.0.0 >/dev/null
done

echo "Producing then consuming a record end-to-end..."
echo "k8s-e2e-test-$(uuidgen)" | kubectl -n "$NAMESPACE" exec -i kcat-producer -- \
    kcat -P -b kafkaesque-headless:9092 -t k8s-e2e-topic \
    -X api.version.request=false -X broker.version.fallback=2.0.0

CONSUMED=$(kubectl -n "$NAMESPACE" exec kcat-producer -- \
    kcat -C -b kafkaesque-headless:9092 -t k8s-e2e-topic -e -c 1 \
    -X api.version.request=false -X broker.version.fallback=2.0.0)

if [[ "$CONSUMED" != k8s-e2e-test-* ]]; then
    echo "FAIL: consumed value did not match produced; got '$CONSUMED'"
    exit 1
fi

echo "PASS: ADVERTISED_HOST E2E"
