from datetime import datetime
from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.sources import source, Webhook
from typing import List


# docsnip dataset_deleted
@meta(owner="mohit@fennel.ai", deleted=True)
@source(Webhook(name="example").endpoint("ticket_sale"))
@dataset
class Ticket:
    ticket_id: str = field(key=True)
    price: int
    at: datetime


# /docsnip
# docsnip gh_action_dataset
@meta(owner="mohit@fennel.ai")
@source(Webhook(name="example").endpoint("ticket_sale"))
@dataset
class Ticket:  # noqa: F811
    ticket_id: str = field(key=True)
    price: int
    at: datetime


# /docsnip
