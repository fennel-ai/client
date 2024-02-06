import pandas as pd


from ci_cd.datasets import Ticket

__owner__ = "mohit@fennel.ai"


# docsnip gh_action_featureset
from fennel import featureset, feature

@featureset
class TicketFeatures:
    ticket_id: str = feature(id=1)
    price: int = feature(id=2).extract(field=Ticket.price, default=0)

# /docsnip
