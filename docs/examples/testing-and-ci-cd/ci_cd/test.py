from datetime import datetime
import pytest


import pandas as pd
from ci_cd.datasets import Ticket
from ci_cd.featuresets import TicketFeatures

from fennel.testing import mock


@mock
def test_featureset_metaflags(client):
    client.commit(
        message="ticket: add first dataset and featureset",
        datasets=[Ticket],
        featuresets=[TicketFeatures],
    )

    df = pd.DataFrame(
        data=[
            ["123", 100, datetime(2020, 1, 1)],
            ["456", 200, datetime(2020, 1, 2)],
        ],
        columns=["ticket_id", "price", "at"],
    )
    client.log("example", "ticket_sale", df)
    feature_df = client.query(
        inputs=[TicketFeatures.ticket_id],
        outputs=[TicketFeatures.price, TicketFeatures.ticket_id],
        input_dataframe=pd.DataFrame(
            data={"TicketFeatures.ticket_id": ["123", "456"]}
        ),
    )
    assert feature_df.to_dict(orient="records") == [
        {"TicketFeatures.ticket_id": "123", "TicketFeatures.price": 100},
        {"TicketFeatures.ticket_id": "456", "TicketFeatures.price": 200},
    ]
