import pandas as pd


from ci_cd.datasets import Ticket

__owner__ = "mohit@fennel.ai"


# docsnip gh_action_featureset
from fennel import featureset, feature


@featureset
class TicketFeatures:
    ticket_id: str
    price: int = feature(Ticket.price, default=0)


# /docsnip
