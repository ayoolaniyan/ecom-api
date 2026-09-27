#!/usr/bin/env bash
# Deletes the local kind cluster created by up.sh, including all of its volumes.
set -euo pipefail

kind delete cluster --name "${CLUSTER:-ecom}"
