import pandas as pd
from fraud.datasets.vehicle import MarketAreaDS
from fraud.featuresets.request import Request

from fennel.featuresets import featureset, feature, extractor
from fennel.lib.schema import inputs, outputs

__owner__ = "eng@app.com"


@featureset
class VehicleFS:
    vehicle_id: int = feature(id=1).extract(feature=Request.vehicle_id)
    market_area_id: int = feature(id=2).extract(
        field=MarketAreaDS.market_area_id, default=0
    )
    vehicle_state: str = feature(id=3)

    @extractor(depends_on=[MarketAreaDS], version=1)
    @inputs(vehicle_id)
    @outputs(vehicle_state)
    def extract_vehicle_state(cls, ts: pd.Series, vehicle_ids: pd.Series):
        market_area_id, _ = MarketAreaDS.lookup(
            ts, vehicle_id=vehicle_ids, fields=["vehicle_state"]
        )
        market_area_id["vehicle_state"] = market_area_id["vehicle_state"].apply(
            lambda x: x[0] if pd.notna(x) else "Unknown"
        )
        return market_area_id[["vehicle_state"]]
