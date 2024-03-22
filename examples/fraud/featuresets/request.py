from fennel.featuresets import featureset, feature as F

__owner__ = "eng@app.com"


@featureset
class Request:
    driver_id: int = F()
    reservation_id: int = F()
    vehicle_id: int = F()
    session_id: str = F()
