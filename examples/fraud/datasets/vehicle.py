from datetime import datetime
from typing import List

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import LastK, Min
from fennel.lib.schema import inputs
from fennel.sources import Webhook, source

__owner__ = "eng@app.com"

webhook = Webhook(name="app_webhook")


@source(webhook.endpoint("LocationDS"), tier="local")
@dataset
class LocationDS:
    id: int = field(key=True)
    latitude: float
    longitude: float
    created: datetime


@dataset
class LocationDS2:
    latitude_int: int = field(key=True)
    longitude_int: int = field(key=True)
    id: int
    created: datetime

    @pipeline(version=1)
    @inputs(LocationDS)
    def location_ds(cls, location: Dataset):
        return (
            location.assign(
                "latitude_int",
                int,
                lambda df: (df["latitude"] * 1000).astype(int),
            )
            .assign(
                "longitude_int",
                int,
                lambda df: (df["longitude"] * 1000).astype(int),
            )
            .drop(["latitude", "longitude"])
            .groupby(["latitude_int", "longitude_int"])
            .first()
        )


@source(webhook.endpoint("LocationToNewMarketArea"), tier="local")
@dataset
class LocationToNewMarketArea:
    gid: int
    latitude: float
    longitude: float
    created: datetime


@dataset
class IdToMarketAreaDS:
    id: int = field(key=True)
    market_area_id: int
    created: datetime

    @pipeline(version=1)
    @inputs(LocationToNewMarketArea, LocationDS2)
    def id_to_market_area(cls, nma: Dataset, location: Dataset):
        return (
            nma.assign(
                "latitude_int",
                int,
                lambda df: (df["latitude"] * 1000).astype(int),
            )
            .assign(
                "longitude_int",
                int,
                lambda df: (df["longitude"] * 1000).astype(int),
            )
            .drop(["latitude", "longitude"])
            .join(
                location,
                on=["latitude_int", "longitude_int"],
                how="inner",
            )
            .rename({"gid": "market_area_id"})
            .drop(["latitude_int", "longitude_int"])
            .groupby(["id"])
            .first()
        )


@source(webhook.endpoint("VehicleSummary"), tier="local")
@dataset
class VehicleSummary:
    vehicle_id: int
    state: str
    longitude: float
    latitude: float
    location_id: int
    created: datetime


@dataset
class MarketAreaDS:
    vehicle_id: int = field(key=True)
    vehicle_state: List[str]
    market_area_id: int
    created: datetime

    @pipeline(version=1)
    @inputs(VehicleSummary, IdToMarketAreaDS)
    def market_area_ds(
        cls, vehicle_summary: Dataset, id_to_market_area: Dataset
    ):
        return (
            vehicle_summary.join(
                id_to_market_area,
                left_on=["location_id"],
                right_on=["id"],
                how="inner",
            )
            .rename({"state": "vehicle_state"})
            .drop("location_id", "longitude", "latitude")
            .groupby(["vehicle_id"])
            .aggregate(
                Min(
                    of="market_area_id",
                    window="forever",
                    default=0,
                    into_field="market_area_id",
                ),
                LastK(
                    of="vehicle_state",
                    window="forever",
                    into_field="vehicle_state",
                    limit=1,
                    dedup=True,
                ),
            )
        )
