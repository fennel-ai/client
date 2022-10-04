import pandas as pd


def aggregate_lookup(agg_name: str, **kwargs):
    print("Dummy Aggregate", agg_name, " lookup patched")
    return pd.Series([8, 12, 13]), pd.Series([21, 22, 23])
