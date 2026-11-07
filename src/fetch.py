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


def generation(a: date, b: date) -> pd.DataFrame:
    pivots, d = [], a
    while d <= b:
        e = min(d + timedelta(days=6), b)
        data = get(f"{BASE}/datasets/FUELINST", {
            "publishDateTimeFrom": f"{d}T00:00:00Z",
            "publishDateTimeTo": f"{e + timedelta(days=1)}T00:00:00Z",
            "format": "json"
        })
        if data:
            cdf = pd.DataFrame(data)
            cdf["ts"] = pd.to_datetime(cdf["startTime"], utc=True)
            cdf["published_at"] = pd.to_datetime(cdf["publishTime"], utc=True)
            cdf["fuelType"] = cdf["fuelType"].str.lower()
            piv = cdf.pivot_table(index="ts", columns="fuelType", values="generation", aggfunc="first")
            fuel_cols = list(piv.columns)
            piv[fuel_cols] = piv[fuel_cols].fillna(0.0)
            piv["published_at"] = cdf.groupby("ts")["published_at"].max()
            pivots.append(piv)
        print("FUELINST", d, e); d = e + timedelta(days=1)
    if not pivots:
        return pd.DataFrame()
    res = pd.concat(pivots)
    return res[~res.index.duplicated(keep="first")].sort_index()


def wind_forecast(a: date, b: date) -> pd.DataFrame:
    rows, d = [], a - timedelta(days=2)
    while d <= b + timedelta(days=1):
        e = min(d + timedelta(days=6), b + timedelta(days=1))
        data = get(f"{BASE}/datasets/WINDFOR", {
            "publishDateTimeFrom": f"{d}T00:00:00Z",
            "publishDateTimeTo": f"{e + timedelta(days=1)}T00:00:00Z",
            "format": "json"
        })
        if data:
            rows.extend(data)
        print("WINDFOR", d, e); d = e + timedelta(days=1)
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["startTime"], utc=True)
    df["published_at"] = pd.to_datetime(df["publishTime"], utc=True)
    df["wind_forecast_mw"] = df["generation"].astype(float)
    df = df.drop_duplicates(subset=["ts", "published_at"])
    return df.set_index("ts").sort_index()[["wind_forecast_mw", "published_at"]]


def demand_forecast(a: date, b: date) -> pd.DataFrame:
    rows, d = [], a - timedelta(days=2)
    while d <= b:
        e = min(d + timedelta(days=6), b)
        data = get(f"{BASE}/forecast/demand/day-ahead/earliest", {
            "from": str(d),
            "to": str(e),
            "boundary": "N",
            "format": "json"
        })
        if data:
            rows.extend(data)
        print("DEMAND", d, e); d = e + timedelta(days=1)
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["startTime"], utc=True)
    df["published_at"] = pd.to_datetime(df["publishTime"], utc=True)
    df["demand_forecast_mw"] = df["nationalDemand"].astype(float)
    df = df.drop_duplicates(subset=["ts", "published_at"])
    return df.set_index("ts").sort_index()[["demand_forecast_mw", "published_at"]]


if __name__ == "__main__":
    a, b = (date.fromisoformat(x) for x in sys.argv[1:3])
    force = "--force" in sys.argv
    if force or not (OUT / "prices.parquet").exists():
        prices(a, b).to_parquet(OUT / "prices.parquet")
    if force or not (OUT / "imbalance.parquet").exists():
        imbalance(a, b).to_parquet(OUT / "imbalance.parquet")
    if force or not (OUT / "windfor.parquet").exists():
        wind_forecast(a, b).to_parquet(OUT / "windfor.parquet")
    if force or not (OUT / "gen.parquet").exists():
        generation(a, b).to_parquet(OUT / "gen.parquet")
    if force or not (OUT / "demand.parquet").exists():
        demand_forecast(a, b).to_parquet(OUT / "demand.parquet")
    print("done ->", OUT)
