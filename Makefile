# Battery Trader — price model. Everything here is model-scoped.
# Feeds / Connect / Flink / Redpanda / control plane / dashboards live in the
# platform repo. See INTERFACE.md for the seam, PLAN.md for the roadmap.

PY ?= python
MODEL ?= mean_by_hour_14d
# ^ `make train MODEL=lgbm` once `make data` has run and the parquet exists.
ARTIFACTS ?= artifacts/gb-price/v1

.PHONY: help test pitch data fixture
.PHONY: train serve-local self-check image eval

help:
	@echo "make test        run the suite (pytest -q -p no:warnings)"
	@echo "make pitch       pitch numbers (python main.py)"
	@echo "make data        refresh the lab parquet (python -m src.fetch)"
	@echo "make fixture     regenerate tests/fixtures/feature_stream.synthetic.jsonl"
	@echo ""
	@echo "Service:"
	@echo "  make train       fit + write $(ARTIFACTS) (MODEL=$(MODEL))"
	@echo "  make serve-local replay the fixture through the service — no broker needed"
	@echo "  make self-check  validate + predict one record (the image gate)"
	@echo "  make image       docker build -f deploy/model/Dockerfile"
	@echo ""
	@echo "Phase 3 (not built yet — see PLAN.md §5):"
	@echo "  make eval        £ / rank IC / bootstrap CI -> outputs/report.md"

test:
	$(PY) -m pytest -q -p no:warnings

pitch:
	$(PY) main.py

data:
	$(PY) -m src.fetch 2026-04-20 2026-11-07

fixture:
	$(PY) scripts/make_fixture.py

# --- Service entry points ----------------------------------------------------
train:
	$(PY) -m src.artifacts train --model $(MODEL) --out $(ARTIFACTS)

serve-local:
	$(PY) -m src.serve --replay tests/fixtures/feature_stream.synthetic.jsonl \
		$(if $(wildcard $(ARTIFACTS)/model.pkl),--artifact $(ARTIFACTS),)

self-check:
	$(PY) -m src.serve --self-check

image:
	docker build -f deploy/model/Dockerfile -t battery/price-model:$$(git rev-parse --short HEAD) .

# eval lands in phase 3:
# eval:
# 	$(PY) -m src.evaluate
