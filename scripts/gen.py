import json
import time
import os
import uuid
from typing import List, Dict, Any
import faker
import random
from datetime import timedelta

# ===============================
# Schema
# ===============================

# @struct
# class Address:
#   city: str
#   street: str
#   state: str
#   zip: str
# 
# @dataset
# class Users:
#   uid: int
#   email: str
#   name: str
#   address: Optional[Address]
#   created_at: datetime
# 
# class TransactionsDS:
#     transaction_id: str
#     timestamp: datetime
#     sender: int
#     recipient: int
#     amount: float
# 
# class FraudMarkingsDS:
#     transaction_id: str
#     is_fraud: bool
#     timestamp: datetime

# ===============================

class Generator:
    def __init__(self, dir1: str, dir2: str, user_file: str, transaction_file: str, fraud_file: str):
        self.faker = faker.Faker(seed=42)
        self.dir1 = dir1
        self.dir2 = dir2
        os.makedirs(dir1, exist_ok=True)
        os.makedirs(dir2, exist_ok=True)
        self.user_file = user_file
        self.transaction_file = transaction_file
        self.fraud_file = fraud_file

    def users(self, n: int):
        faker = self.faker
        ret = []
        for _ in range(n):
            this = {
                "uid": faker.random_int(min=1, max=100_000_000),
                "name": faker.name(),
                "email": faker.email(),
                "created_at": faker.date_time_this_decade(),
            }
            if random.random() < 0.7:
                this["address"] = {
                    "street": faker.street_address(),
                    "city": faker.city(),
                    "state": faker.administrative_unit(),
                    "zip": faker.postcode(),
                }
            ret.append(this)
        return ret

    def transactions(self, n: int, users: List[Dict[str, Any]]):
        faker = self.faker
        ret = []
        for _ in range(n):
            this = {
                "transaction_id": str(uuid.uuid4()),
                "sender": random.choice(users)["uid"],
                "receiver": random.choice(users)["uid"],
                "amount": random.randint(1, 1000_000) / 100.0,
                "timestamp": faker.date_time_this_year(),
            }
            ret.append(this)
        return ret

    def fraud_markings(self, frac: float, transactions: List[Dict[str, Any]]):
        faker = self.faker
        ret = []
        for t in transactions:
            if random.random() < frac:
                delay = random.randint(0, 3600 * 24 * 1)
                ret.append({
                    "transaction_id": t["transaction_id"],
                    "is_fraud": random.random() < 0.5,
                    "timestamp": t["timestamp"] + timedelta(seconds=delay),
                })
        return ret

    def write_json(self, users: List[Dict[str, Any]], transactions: List[Dict[str, Any]], fraud_markings: List[Dict[str, Any]]):
        users1, users2 = split(users, 0.5)
        transactions1, transactions2 = split(transactions, 0.5)
        fraud_markings1, fraud_markings2 = split(fraud_markings, 0.5)
        groups = [
            (self.dir1, users1, transactions1, fraud_markings1), 
            (self.dir2, users2, transactions2, fraud_markings2)
        ]
        for (dir, users, transactions, fraud_markings) in groups:
            os.makedirs(dir, exist_ok=True)
            with open(os.path.join(dir, self.user_file), "w") as f:
                for u in users:
                    u["created_at"] = u["created_at"].isoformat()
                    f.write(json.dumps(u) + "\n")
            with open(os.path.join(dir, self.transaction_file), "w") as f:
                for t in transactions:
                    t["timestamp"] = t["timestamp"].isoformat()
                    f.write(json.dumps(t) + "\n")
            with open(os.path.join(dir, self.fraud_file), "w") as f:
                for marking in fraud_markings:
                    marking["timestamp"] = marking["timestamp"].isoformat()
                    f.write(json.dumps(marking) + "\n")

def split(data: List[Any], frac: float):
    n = len(data)
    n1 = int(n * frac)
    return data[:n1], data[n1:]

if __name__ == "__main__":
    start = time.time()
    num_users = 10_000
    num_transactions = 1000_000
    frac_fraud = 0.01

    g = Generator(dir1="data/s3", dir2="data/kafka", user_file="users.json", transaction_file="transactions.json", fraud_file="fraud.json")
    users = g.users(num_users)
    print(f"Generated users in {time.time() - start} seconds")
    start = time.time()
    transactions = g.transactions(num_transactions, users)
    print(f"Generated transactions in {time.time() - start} seconds")
    start = time.time()
    fraud_markings = g.fraud_markings(frac_fraud, transactions)
    print(f"Generated fraud markings in {time.time() - start} seconds")
    start = time.time()
    g.write_json(users, transactions, fraud_markings)
    print(f"Wrote to disk in {time.time() - start} seconds")
