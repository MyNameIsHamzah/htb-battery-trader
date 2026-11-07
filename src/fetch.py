"""Pull real data from Elexon (free, no key). OWNER: A.

    python -m src.fetch 2026-04-20 2026-11-07

Writes data/prices.parquet (price) and data/imbalance.parquet (ssp, sbp, niv),
index ts (UTC period start). Endpoints tested 2026-11-07.
TODO(A): FUELINST -> data/gen.parquet, WINDFOR -> data/windfor.parquet,
         both WITH a published_at column.
"""
import sys, time
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
import requests

BASE = "https://data.elexon.co.uk/bmrs/api/v1"
OUT = Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)
S = requests.Session()


def get(url, params=None):
    for attempt in range(4):
        r = S.get(url, params=params, timeout=30)
        if r.ok:
            return r.json()["data"]
        time.sleep(2 * (attempt + 1))
    r.raise_for_status()


def prices(a: date, b: date) -> pd.DataFrame:
    rows, d = [], a
    while d <= b:
        e = min(d + timedelta(days=6), b)
        rows += get(f"{BASE}/datasets/MID", {"from": f"{d}T00:00Z",
                    "to": f"{e + timedelta(days=1)}T00:00Z", "format": "json"})
        print("MID", d, e); d = e + timedelta(days=1)
    df = pd.DataFrame(rows)
    df = df[(df.dataProvider == "APXMIDP") & (df.volume > 0)]  # drop zero-volume, NOT £0 prices
    df["ts"] = pd.to_datetime(df.startTime, utc=True)
    return df.drop_duplicates("ts").set_index("ts").sort_index()[["price", "volume"]]


def imbalance(a: date, b: date) -> pd.DataFrame:
    rows, d = [], a
    while d <= b:
        rows += get(f"{BASE}/balancing/settlement/system-prices/{d}", {"format": "json"})
        print("SP", d); d += timedelta(days=1)
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df.startTime, utc=True)
    df = df.rename(columns={"systemSellPrice": "ssp", "systemBuyPrice": "sbp", "netImbalanceVolume": "niv"})
    return df.drop_duplicates("ts").set_index("ts").sort_index()[["ssp", "sbp", "niv"]]


if __name__ == "__main__":
    a, b = (date.fromisoformat(x) for x in sys.argv[1:3])
    prices(a, b).to_parquet(OUT / "prices.parquet")
    imbalance(a, b).to_parquet(OUT / "imbalance.parquet")
    print("done ->", OUT)
