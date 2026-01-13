import argparse, json, os
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--out_csv", required=True)
args = ap.parse_args()

row = json.loads(input().strip())
df = pd.DataFrame([row])

if os.path.exists(args.out_csv):
    df.to_csv(args.out_csv, mode="a", header=False, index=False)
else:
    df.to_csv(args.out_csv, index=False)
