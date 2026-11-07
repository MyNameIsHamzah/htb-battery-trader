#!/usr/bin/env bash
# VM bootstrap for the Battery Trader streaming stack (SPEC-streaming.md Section 1).
# Target: Ubuntu 24.04, >= 16 GB RAM, >= 4 vCPU. Safe to re-run.
#
#   bash deploy/bootstrap.sh
#
# Installs: docker (+compose), kubectl, helm, k3d, rpk. Then pre-pulls every image
# so the venue wifi can die later. Run this the moment the VM is reachable.
set -euo pipefail

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This script targets Linux (Ubuntu 24.04). Aborting." >&2
  exit 1
fi

log "Base packages"
sudo apt-get update -y
sudo apt-get install -y curl ca-certificates gnupg unzip jq

if ! command -v docker >/dev/null 2>&1; then
  log "Installing Docker"
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
else
  log "Docker present: $(docker --version)"
fi

if ! command -v kubectl >/dev/null 2>&1; then
  log "Installing kubectl v1.29.6 (compatible with k3s shipped by k3d)"
  curl -LO "https://dl.k8s.io/release/v1.29.6/bin/linux/amd64/kubectl"
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
  rm -f kubectl
else
  log "kubectl present: $(kubectl version --client --output=yaml 2>/dev/null | awk '/gitVersion/{print $2; exit}')"
fi

if ! command -v helm >/dev/null 2>&1; then
  log "Installing helm"
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
else
  log "helm present: $(helm version --short)"
fi

if ! command -v k3d >/dev/null 2>&1; then
  log "Installing k3d"
  curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
else
  log "k3d present: $(k3d version | head -1)"
fi

if ! command -v rpk >/dev/null 2>&1; then
  log "Installing rpk"
  curl -sL https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip -o /tmp/rpk.zip
  unzip -o /tmp/rpk.zip -d /tmp/rpk
  sudo install /tmp/rpk/rpk /usr/local/bin/rpk
  rm -f /tmp/rpk.zip
else
  log "rpk present: $(rpk version 2>/dev/null | head -1)"
fi

log "Installed versions"
docker --version   || true
kubectl version --client 2>/dev/null | head -1 || true
helm version --short || true
k3d version | head -1 || true
rpk version 2>/dev/null | head -1 || true

# Pre-pull images so we do not depend on the venue network during the demo.
# VERIFY / pin these tags against deploy/*.yaml before the run.
log "Pre-pulling images (edit IMAGES if you pin different tags)"
IMAGES=(
  "docker.redpanda.com/redpandadata/redpanda:latest"
  "docker.redpanda.com/redpandadata/console:latest"
  "ghcr.io/apache/flink-kubernetes-operator:1.9.0"
  "docker.io/flink:1.19.1-scala_2.12-java11"
  "docker.io/python:3.12-slim"
)
for img in "${IMAGES[@]}"; do
  docker pull "$img" || echo "WARN: failed to pull $img (pin/verify the tag)"
done

log "Bootstrap complete."
echo "If 'docker' needs sudo, run:  newgrp docker   (or log out and back in)"
echo "Next:  make up"
