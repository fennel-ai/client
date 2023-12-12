# docsnip gh_action_sync
import os
from fennel.client import Client

from ci_cd.datasets import Ticket
from ci_cd.featuresets import TicketFeatures

if __name__ == "__main__":
    url = os.getenv("FENNEL_URL")
    token = os.getenv("FENNEL_TOKEN")

    client = Client(url=url, token=token)
    client.sync(datasets=[Ticket], featuresets=[TicketFeatures])

# /docsnip
