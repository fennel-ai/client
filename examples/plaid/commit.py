from features import PlaidReport

TOKEN = "9nWHJMVT6YyaImFotfdCzhzHD30WE3ywh3CQIF0_zUU"
URL = "https://hoang.aws.fennel.ai/"
BRANCH = "plaid_test"

if __name__ == "__main__":
    from fennel.client import Client

    client = Client(url=URL, token=TOKEN)
    client.checkout(BRANCH, init=True)
    client.commit("add plaid report features", featuresets=[PlaidReport])
