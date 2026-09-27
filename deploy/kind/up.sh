#!/usr/bin/env bash
# Creates a local kind cluster and deploys the full stack: the Strimzi operator, a 3-node
# Kafka cluster, Redis and the API. Safe to re-run; each step is idempotent.
#
# Requires: docker, kind, kubectl, helm.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${CLUSTER:-ecom}"
NAMESPACE="${NAMESPACE:-ecom}"
RELEASE="${RELEASE:-ecom-api}"
STRIMZI_NAMESPACE="${STRIMZI_NAMESPACE:-strimzi}"
STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
IMAGE="ecomapi:local"
CONTEXT="kind-${CLUSTER}"

kc() { kubectl --context "$CONTEXT" "$@"; }
hc() { helm --kube-context "$CONTEXT" "$@"; }

echo "==> kind cluster '${CLUSTER}'"
if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --config "$ROOT/deploy/kind/cluster.yaml"
fi

echo "==> Building and loading ${IMAGE}"
docker build -t "$IMAGE" "$ROOT/EcomAPI"
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> Namespace '${NAMESPACE}'"
kc create namespace "$NAMESPACE" --dry-run=client -o yaml | kc apply -f -

echo "==> Strimzi Cluster Operator ${STRIMZI_VERSION} (watching '${NAMESPACE}')"
helm repo add strimzi https://strimzi.io/charts/ >/dev/null 2>&1 || true
helm repo update strimzi >/dev/null
hc upgrade --install strimzi-operator strimzi/strimzi-kafka-operator \
  --version "$STRIMZI_VERSION" \
  --namespace "$STRIMZI_NAMESPACE" --create-namespace \
  --set "watchNamespaces={${NAMESPACE}}" \
  --wait --timeout 5m

echo "==> ecom-api chart"
# No --wait: Helm 4 also waits on the Strimzi custom resources, and the first Kafka
# reconciliation (image pulls, CA generation) can outlast it. Readiness is awaited below.
hc upgrade --install "$RELEASE" "$ROOT/deploy/helm/ecom-api" \
  --namespace "$NAMESPACE" \
  -f "$ROOT/deploy/helm/ecom-api/values-local.yaml"

# The image tag doesn't change between builds, so restart to pick up a rebuilt image.
kc -n "$NAMESPACE" rollout restart deployment "$RELEASE"
kc -n "$NAMESPACE" rollout status deployment "$RELEASE" --timeout 3m

echo "==> Waiting for Kafka and the topic"
kc -n "$NAMESPACE" wait kafka "${RELEASE}-kafka" --for=condition=Ready --timeout 10m
kc -n "$NAMESPACE" wait kafkatopic --all --for=condition=Ready --timeout 3m

echo "==> Smoke test"
hc test "$RELEASE" --namespace "$NAMESPACE" --logs

echo
echo "API: http://localhost:5026 (e.g. curl http://localhost:5026/api/orders)"
echo "Tear down with: $ROOT/deploy/kind/down.sh"
