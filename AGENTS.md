# AGENTS.md — every agent reads this first (Cline, Claude Code, Cursor, Aider…)

Project: Battery Trader — **price model service only**. Full spec: `SPEC.md`.
Repo focus + roadmap: `PLAN.md`. Contract with the platform repo: `INTERFACE.md`.
Your task: `tasks/<your letter>.md`.

## Before any change
1. Read `SPEC.md` sections relevant to your task + your `tasks/X.md`. Nothing else unless needed.
2. Run `pytest -q -p no:warnings` and `python main.py` — both must pass/run before AND after your change.

## Hard rules
- **This repo is the price model and its objective function. Nothing else.** Feeds,
  Connect, Flink, Redpanda, topic provisioning, the control plane and dashboards live in
  the platform repo. Do not add them back: no `kubectl`, no `rpk`, no HTTP API, no Streamlit.
- **Only edit files your task file lists as yours.**
- **NEVER edit `src/config.py`, function signatures in `src/*.py`, or anything in `tests/` except your own new test file.** Contracts are frozen. Need a change? Stop and tell the human.
- **NEVER delete, skip, or loosen a test to make it pass.** Fix the code.
- No lookahead: day-ahead features only use data published before 11:00 UTC on D-1.
- NEVER print/cat data files. Use `python scripts/peek.py <file>`.
- No new dependencies without saying so.
- Small diffs. Run tests. Don't claim it works without running it.
- Stuck after 2 failed attempts at the same fix: stop, summarise, ask.
- Don't commit or push. The human does.

## Commands
- Tests: `pytest -q -p no:warnings`
- Pitch numbers: `python main.py`
- Data (lab only): `python -m src.fetch 2026-04-20 2026-11-07`
- Everything else: `make help` (train / serve-local / self-check / image exist now; eval lands in phase 3, see `PLAN.md` §5)

## Git (humans)
Push to main. `git pull --rebase` before push. Commit small and often. Main must always run `python main.py`. Merge main → `demo` branch whenever it works end to end.
