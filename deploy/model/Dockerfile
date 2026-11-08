# The price model service. Ships the model, not the lab: no data, no tests, no tooling.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ARTIFACT_DIR=/app/artifacts/gb-price/v1 \
    FEATURES_TOPIC=features.gb-price.v1 \
    PREDICTIONS_TOPIC=predictions.gb-price.v1 \
    CONSUMER_GROUP=model-gb-price-v1

WORKDIR /app

# Deps first so the model/code layers rebuild without a re-install.
COPY requirements.txt requirements-serve.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-serve.txt \
    && useradd --create-home --uid 10001 model

# Code. `src/fetch.py` and the lab modules need the internet and parquet; they are
# never imported by `src.serve`, which is why the image can be scratch-thin.
COPY src/ ./src/
COPY INTERFACE.md ./

# The artifact is baked in: `make train` must have run before `make image`.
COPY artifacts/ ./artifacts/

USER model

# Build gate: if the image cannot validate + predict one record with no data, no
# broker and no network, the build fails here instead of in production.
RUN python -m src.serve --self-check

ENTRYPOINT ["python", "-m", "src.serve"]
CMD ["--kafka"]
