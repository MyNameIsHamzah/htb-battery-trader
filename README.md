# Battery Trader — HackTheBurgh XIII (HRT track)
We turn wind forecasts into money: a trading brain for a grid battery. See `SPEC.md`.

```bash
pip install -r requirements.txt
python -m src.fetch 2026-04-20 2026-11-07   # real data (else synthetic is used)
python main.py                              # prints the pitch numbers
pytest -q                                   # guard rails
streamlit run app/dashboard.py
```
