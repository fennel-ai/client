from fennel.client import Client
import threading
import pandas as pd
import time
import json
from typing import Any
from features import PlaidReport

TOKEN = "9nWHJMVT6YyaImFotfdCzhzHD30WE3ywh3CQIF0_zUU"
URL = "https://hoang.aws.fennel.ai/"
BRANCH = "plaid_test"


def read_json(path: str) -> Any:
    with open(path, "r") as f:
        return f.read()


def traffic(t: int):
    client = Client(url=URL, token=TOKEN)
    client.checkout(BRANCH)
    payload = read_json("sample.json")
    print(f"Thread {t} started")
    n = 0
    while True:
        if n % 100 == 0:
            print(f"Thread {t} processing {n} requests")
        once(client, payload)
        n += 1


def once(client: Client, payload: str):
    start = time.time()
    df = client.query(
        inputs=[PlaidReport.payload],
        outputs=[
            PlaidReport.num_transactions,
            PlaidReport.num_ecomerce_txns,
            PlaidReport.total_ecomerce_spend,
            PlaidReport.any_ecomerce_txn,
            PlaidReport.num_atm_txns,
            PlaidReport.total_atm_spend,
            PlaidReport.any_atm_txn,
            PlaidReport.num_retail_txns,
            PlaidReport.total_retail_spend,
            PlaidReport.any_retail_txn,
        ],
        input_dataframe=pd.DataFrame({"PlaidReport.payload": [payload]}),
    )
    end = time.time()
    print(df)
    print(f"Time taken: {end - start} seconds")


if __name__ == "__main__":
    client = Client(url=URL, token=TOKEN)
    client.checkout(BRANCH)
    payload = read_json("sample.json")
    once(client, payload)
