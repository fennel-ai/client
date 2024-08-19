from fennel.featuresets import featureset, feature

__owner__ = "eng@app.com"


@featureset
class Request:
    driver_id: int
    reservation_id: int
    vehicle_id: int
    session_id: str
