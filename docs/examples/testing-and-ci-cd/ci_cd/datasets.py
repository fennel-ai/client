from datetime import datetime
from typing import List

from fennel.datasets import index


def test_dataset_deleted():
    # docsnip dataset_deleted
    from fennel.datasets import dataset, field
    from fennel.lib import meta
    from fennel.connectors import source, Webhook

    @meta(owner="mohit@fennel.ai", deleted=True)
    @source(
        Webhook(name="example").endpoint("ticket_sale"),
        disorder="14d",
        cdc="upsert",
    )
    @index
    @dataset
    class Ticket:
        ticket_id: str = field(key=True)
        price: int
        at: datetime

    # /docsnip


def test_gh_actions_dataset():
    # docsnip gh_action_dataset
    from fennel.datasets import dataset, field
    from fennel.lib import meta
    from fennel.connectors import source, Webhook

    @meta(owner="mohit@fennel.ai")
    @source(
        Webhook(name="example").endpoint("ticket_sale"),
        disorder="14d",
        cdc="upsert",
    )
    @index
    @dataset
    class Ticket:
        ticket_id: str = field(key=True)
        price: int
        at: datetime

    # /docsnip
    return Ticket


Ticket = test_gh_actions_dataset()
