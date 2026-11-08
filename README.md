# Battery Trader — the price model

We turn wind forecasts into money: a trading brain for a grid battery.

This repo is the **model service**: it consumes one feature event per settlement period,
predicts the price, and publishes a prediction event — that's it. Feeds, Connect, Flink,
Redpanda, topic provisioning, the control plane and dashboards live in the platform repo.
The seam between the two is `INTERFACE.md`.

Docs: `INTERFACE.md` (the two record contracts) · `PLAN.md` (roadmap, phased) ·
`SPEC.md` (the model and how it is judged: **£ first, MAE second**).

```bash
pip install -r requirements.txt
python -m src.fetch 2026-04-20 2026-11-07   # real data (else synthetic is used)
python main.py                              # the pitch table: £, rank, % of perfect
pytest -q                                   # guard rails
make help                                   # everything else
```
