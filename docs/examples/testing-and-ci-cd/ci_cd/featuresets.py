import pandas as pd


from ci_cd.datasets import Ticket

__owner__ = "mohit@fennel.ai"


# docsnip gh_action_featureset
from fennel.featuresets import featureset, feature as F


@featureset
class TicketFeatures:
    ticket_id: str
    price: int = F(Ticket.price, default=0)


# /docsnip
