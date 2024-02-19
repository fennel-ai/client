import pandas as pd

from fennel import featureset, extractor, feature
from fennel.lib.metadata import meta
from fennel.lib import inputs, outputs

from ci_cd.datasets import Ticket


# docsnip gh_action_featureset
@meta(owner="mohit@fennel.ai")
@featureset
class TicketFeatures:
    ticket_id: str = feature(id=1)
    price: int = feature(id=2).extract(field=Ticket.price, default=0)


# /docsnip
