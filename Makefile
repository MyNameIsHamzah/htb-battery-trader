# Battery Trader — Stream-to-Model Control Plane runner (SPEC-streaming.md).
# Linux VM only (Docker + k3d). Recipes are TAB-indented.
#
#   make up            # cluster + Redpanda + schema registry + all topics (atomic)
#   make wind-demo     # first vertical slice: wind feed -> Flink -> features.wind-demo.v1
#   make down          # tear everything down

SHELL  := /bin/bash
SPEED  ?= 1000
NS     := battery
BROKER := redpanda-0.redpanda.redpanda.svc.cluster.local:9093
SR     := http://redpanda-0.redpanda.redpanda.svc.cluster.local:8081
API    := http://localhost:8000

.DEFAULT_GOAL := help
.PHONY: help bootstrap up infra topics schemas feeds down deploy-apps \
        feed-wind query-wind-demo wind-demo replay status metrics catalog \
        scale topic-list

help: ## Show available targets
	@echo "Stream-to-Model control plane — targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Install docker/kubectl/helm/k3d/rpk + pre-pull images (run once)
	bash deploy/bootstrap.sh

infra: ## Create k3d cluster + deploy Redpanda (+ built-in schema registry)
	k3d cluster create battery --servers 1 --agents 2 -p "8081:80@loadbalancer" || echo "cluster already exists"
	helm repo add redpanda https://charts.redpanda.com || true
	helm repo update
	helm upgrade --install redpanda redpanda/redpanda -n redpanda --create-namespace -f deploy/redpanda-values.yaml
	kubectl -n redpanda rollout status statefulset/redpanda --timeout=300s

topics: ## Create all feed + feature + prediction topics (idempotent)
	kubectl create namespace $(NS) --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n $(NS) delete job redpanda-create-topics --ignore-not-found
	kubectl apply -f deploy/topics-job.yaml
	kubectl -n $(NS) wait --for=condition=complete job/redpanda-create-topics --timeout=180s
	kubectl -n $(NS) logs job/redpanda-create-topics --tail=40

up: infra topics ## One-shot: cluster + broker + schema registry + topics (atomic)

schemas: ## Register feed/feature/prediction schemas via the control API
	@curl -s -X POST $(API)/schemas || echo "control-api not up yet — run 'make deploy-apps' first (Section 6)"

feeds: ## Apply all Redpanda Connect ingestion pipelines
	@if ls deploy/connect/*.yaml >/dev/null 2>&1; then kubectl apply -f deploy/connect/; else echo "no connect pipelines yet"; fi

feed-wind: ## Slice step 1: create elexon-wind-forecast feed (topic + Connect pipeline)
	kubectl apply -f deploy/connect/wind_forecast.yaml
	@echo "wind feed pipeline applied. Schema + catalogue register via POST $(API)/feeds once control-api is live."

query-wind-demo: ## Slice step 2: register the wind-demo model query -> features.wind-demo.v1
	@if curl -sf $(API)/health >/dev/null 2>&1; then \
	  curl -s -X POST $(API)/model-queries -H 'content-type: application/json' --data @deploy/flink/wind_demo_model_query.json; \
	elif [ -f scripts/submit_sql.py ]; then python scripts/submit_sql.py deploy/flink/wind_demo_query.sql; \
	else echo "need control-api or scripts/submit_sql.py (Section 6/7)"; fi

wind-demo: feed-wind query-wind-demo ## End-to-end wind slice: feed -> Flink -> features.wind-demo.v1
	@echo "--- consuming features.wind-demo.v1 (5 records) ---"
	@rpk topic consume features.wind-demo.v1 --brokers $(BROKER) -n 5 || echo "no feature records yet"

deploy-apps: ## Deploy the control-api (and future apps)
	@if [ -d deploy/apps ]; then kubectl apply -f deploy/apps/; else echo "no app manifests yet"; fi

replay: ## Replay historical feeds at SPEEDx (needs stream/producer_replay.py)
	@if [ -f stream/producer_replay.py ]; then SPEED=$(SPEED) python -m stream.producer_replay; else echo "stream/producer_replay.py not implemented yet (Section 3)"; fi

status: ## Show pods + topics
	@echo "--- redpanda ---"; kubectl -n redpanda get pods || true
	@echo "--- battery ---";  kubectl -n $(NS) get pods || true
	@echo "--- topics ---";   rpk topic list --brokers $(BROKER) || true

metrics: ## Print control-api /metrics
	@curl -s $(API)/metrics | python -m json.tool || echo "control-api not up"

catalog: ## Print the feed catalogue
	@curl -s $(API)/catalog | python -m json.tool || echo "control-api not up"

scale: ## Scale a model query: make scale JOB=wind-demo REPLICAS=6
	@curl -s -X POST "$(API)/model-queries/$(JOB)/scale?replicas=$(REPLICAS)" || echo "control-api not up"

topic-list: ## List topics via rpk
	rpk topic list --brokers $(BROKER)

down: ## Tear down the k3d cluster (removes Redpanda, topics, all workloads)
	k3d cluster delete battery || echo "no cluster"

