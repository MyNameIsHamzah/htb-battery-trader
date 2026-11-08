"""How far back does each Elexon dataset actually go? OWNER: A (data).

    python scripts/coverage.py                 # API depth probe (mid-Jan, 2020..2026)
    python scripts/coverage.py --local         # + local data/*.parquet coverage
    python scripts/coverage.py --budget 2023-01-01 2026-11-07

Read-only: hits the public Insights API (no key) and reads data/*.parquet.
Prints row counts and date ranges only — never raw rows (AGENTS.md).

Why this exists: extending history is only useful for the datasets that HAVE
history. Probed 2026-11-08, the floors are NOT uniform:

    MID (price)            from ~2016-10   (0 rows at 2016-07)
    system-prices (ssp)    from ~2016-01
    FUELINST (gen mix)     from ~2017-01
    WINDFOR (wind fcst)    from ~2016-07   (NOT shallow — 196+ revisions/day)
    demand-fcst (day-ahead) from ~2022     <-- the binding constraint

So 2022-01-01 is the latest date where ALL five sources exist. Going further
back buys days but the `demand_forecast_mw` feature is NaN for them.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.fetch import BASE, S  # noqa: E402

DATA = ROOT / "data"
PROBE_MONTH_DAY = (1, 15)  # mid-January: no clock changes, no holidays
DEFAULT_YEARS = list(range(2026, 2015, -1))  # 2016..2026 — wide enough to hit the floor


def _probe(url: str, params: dict) -> int | str:
    """Rows returned, or a short 'HTTP nnn' marker. One shot, no retry loop."""
    r = S.get(url, params=params, timeout=20)
    if r.status_code != 200:
        return f"HTTP {r.status_code}"
    return len(r.json().get("data") or [])


def mid(d: date) -> int | str:
    return _probe(f"{BASE}/datasets/MID", {
        "from": f"{d}T00:00Z", "to": f"{d + timedelta(days=1)}T00:00Z",
        "format": "json"})


def system_prices(d: date) -> int | str:
    return _probe(f"{BASE}/balancing/settlement/system-prices/{d}", {"format": "json"})


def fuelinst(d: date) -> int | str:
    return _probe(f"{BASE}/datasets/FUELINST", {
        "publishDateTimeFrom": f"{d}T00:00:00Z",
        "publishDateTimeTo": f"{d + timedelta(days=1)}T00:00:00Z", "format": "json"})


def windfor(d: date) -> int | str:
    return _probe(f"{BASE}/datasets/WINDFOR", {
        "publishDateTimeFrom": f"{d}T00:00:00Z",
        "publishDateTimeTo": f"{d + timedelta(days=1)}T00:00:00Z", "format": "json"})


def demand_forecast(d: date) -> int | str:
    return _probe(f"{BASE}/forecast/demand/day-ahead/earliest", {
        "from": str(d), "to": str(d), "boundary": "N", "format": "json"})


PROBES = {
    "MID (price)": mid,
    "system-prices (ssp/sbp)": system_prices,
    "FUELINST (gen mix)": fuelinst,
    "WINDFOR (wind fcst)": windfor,
    "demand-fcst (day-ahead)": demand_forecast,
}


def probe(years: list[int]) -> pd.DataFrame:
    days = [date(y, *PROBE_MONTH_DAY) for y in years]
    table: dict[str, dict[str, int | str]] = {}
    for name, fn in PROBES.items():
        row: dict[str, int | str] = {}
        for d in days:
            try:
                row[d.isoformat()] = fn(d)
            except Exception as exc:  # network / JSON shape
                row[d.isoformat()] = f"ERR {type(exc).__name__}"
            print(f"  {name:24s} {d} -> {row[d.isoformat()]}", flush=True)
        table[name] = row
    return pd.DataFrame(table).T


def deepest(tbl: pd.DataFrame) -> pd.Series:
    """Earliest probe date that still returned rows (probe grid = mid-Jan)."""

    def earliest(row: pd.Series) -> str:
        ok = [(d, v) for d, v in row.items() if isinstance(v, int) and v > 0]
        return min(ok)[0] if ok else "none"

    return tbl.apply(earliest, axis=1).rename("earliest_probe_with_data")


def local() -> pd.DataFrame:
    rows = []
    for p in sorted(DATA.glob("*.parquet")):
        df = pd.read_parquet(p)
        rows.append({
            "file": p.name,
            "rows": len(df),
            "from": str(df.index.min()),
            "to": str(df.index.max()),
            "published_at": (
                f"{df['published_at'].min()} .. {df['published_at'].max()}"
                if "published_at" in df.columns else "-"
            ),
        })
    return pd.DataFrame(rows)


def budget(start: date, end: date) -> None:
    days = (end - start).days + 1
    print(f"\nfeature-row budget {start}..{end}: {days} days x 48 = {days * 48:,} rows")
    print("  (one feature row = one 30-min settlement period; ~1 independent "
          "sample per DAY for model ranking, not per row)")


def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    ap.add_argument("--local", action="store_true", help="also report local parquet coverage")
    ap.add_argument("--budget", nargs=2, metavar=("START", "END"),
                    help="estimate feature rows for a date range")
    a = ap.parse_args(argv)

    print("API depth probe — rows returned for one day in mid-January:\n")
    tbl = probe(sorted(a.years))
    print("\n" + tbl.to_string())
    print("\nDeepest usable history (earliest probe date with data):\n")
    print(deepest(tbl).to_string())
    print("  (the grid samples mid-January only, so the true first date is "
          "somewhere between this year and the year before it)")

    if a.local:
        print("\nLocal parquet coverage:\n")
        print(local().to_string(index=False))

    if a.budget:
        budget(*(date.fromisoformat(x) for x in a.budget))


if __name__ == "__main__":
    main(sys.argv[1:])
