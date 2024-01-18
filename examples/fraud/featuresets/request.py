from fennel.featuresets import featureset, feature, extractor
from fennel.lib.schema import inputs, outputs
from fennel.lib.schema import oneof

__owner__ = "eng@app.com"


@featureset
class Request:
    driver_id: int = feature(id=1)
    reservation_id: int = feature(id=2)
    vehicle_id: int = feature(id=3)
    session_id: str = feature(id=4)
