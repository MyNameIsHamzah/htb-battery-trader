"""Safe data peek for agents: python scripts/peek.py data/file.csv"""
import sys, pandas as pd
p = sys.argv[1]
df = pd.read_parquet(p) if p.endswith(".parquet") else pd.read_csv(p, nrows=100_000)
print(df.shape); print(df.dtypes); print(df.head()); print(df.describe().T)
