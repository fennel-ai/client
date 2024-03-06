from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.datasets import dataset, Dataset, field, pipeline, LastK
from fennel.dtypes import struct
from fennel.featuresets import featureset, feature, extractor
from fennel.lib import inputs, outputs
from fennel.sources import Webhook, source
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "nitin@fennel.ai"


@struct
class Lookup:
    movie_id: int


@struct
class Role:
    role_id: int
    name: str
    cost: int


@struct
class RoleBudget:
    role_id: int
    count: int
    total_cost: int


@struct
class MovieBudget:
    roles: List[RoleBudget]


@source(webhook.endpoint("MovieDS"), cdc="append", disorder="14d")
@dataset
class MovieDS:
    movie_id: int
    director_id: int
    role_id: int
    name: str
    cost: int
    timestamp: datetime = field(timestamp=True)


@dataset
class MovieInfo:
    director_id: int = field(key=True)
    movie_id: int = field(key=True)
    role_list: List[Role]
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(MovieDS)
    def movie_info(cls, movie: Dataset):
        return (
            movie.assign(
                name="role",
                dtype=Role,
                func=lambda x: x[["role_id", "name", "cost"]].apply(
                    lambda z: Role(
                        **{"role_id": z[0], "name": z[1], "cost": z[2]}
                    ),
                    axis=1,
                ),
            )
            .drop(columns=["role_id", "name", "cost"])
            .groupby("director_id", "movie_id")
            .aggregate(
                LastK(
                    into_field="role_list",
                    of="role",
                    window="forever",
                    limit=3,
                    dedup=False,
                ),
            )
        )


@featureset
class Request:
    director_id: int = feature(id=1)
    movie_id: int = feature(id=2)
    director_movie_id: Lookup = feature(id=3)


@featureset
class MovieFeatures:
    role_list_py: List[Role] = feature(id=1)
    role_list_assign: List[Role] = feature(id=2).extract(field=MovieInfo.role_list, default=[], provider=Request)  # type: ignore
    role_list_struct: List[Role] = feature(id=3)
    movie_budget: MovieBudget = feature(id=4)

    @extractor(depends_on=[MovieInfo])
    @inputs(Request.director_id, Request.movie_id)
    @outputs(role_list_py)
    def extract_cast(
        cls, ts: pd.Series, director_ids: pd.Series, movie_ids: pd.Series
    ):
        res, _ = MovieInfo.lookup(ts, director_id=director_ids, movie_id=movie_ids)  # type: ignore
        res = res.rename(columns={"role_list": "role_list_py"})
        return pd.Series(res["role_list_py"].fillna("").apply(list))

    @extractor(depends_on=[MovieInfo])
    @inputs(Request.director_movie_id)
    @outputs(role_list_struct)
    def extract_cast_struct(cls, ts: pd.Series, director_movie_ids: pd.Series):
        director_ids = pd.Series(
            director_movie_ids.apply(lambda x: x["director_id"])
        )
        movie_ids = pd.Series(director_movie_ids.apply(lambda x: x["movie_id"]))
        res, _ = MovieInfo.lookup(ts, director_id=director_ids, movie_id=movie_ids)  # type: ignore
        res = res.rename(columns={"role_list": "role_list_struct"})
        return pd.Series(res["role_list_struct"].fillna("").apply(list))

    @extractor(depends_on=[MovieInfo])
    @inputs(Request.director_id, Request.movie_id)
    @outputs(movie_budget)
    def extract_movie_budget(
        cls, ts: pd.Series, director_ids: pd.Series, movie_ids: pd.Series
    ):
        res, _ = MovieInfo.lookup(ts, director_id=director_ids, movie_id=movie_ids)  # type: ignore
        output = []
        for roles in res["role_list"].fillna("").apply(list).tolist():
            role_id_cost_map: Dict[int, RoleBudget] = {}
            for role in roles:
                if role.role_id in role_id_cost_map:
                    role_id_cost_map[role.role_id].count += 1
                    role_id_cost_map[role.role_id].total_cost += role.cost
                else:
                    role_id_cost_map[role.role_id] = RoleBudget(
                        role_id=role.role_id,
                        count=1,
                        total_cost=role.cost,
                    )  # type: ignore
            if role_id_cost_map.values():
                output.append(
                    MovieBudget(roles=list(role_id_cost_map.values()))  # type: ignore
                )
            else:
                output.append(MovieBudget(roles=[]))  # type: ignore
        res["movie_budget"] = output
        return pd.Series(res["movie_budget"])


def _log_movie_data(client):
    now = datetime.utcnow()
    data = [
        {
            "movie_id": 1,
            "director_id": 1,
            "role_id": 1,
            "name": "Actor1",
            "cost": 1000,
            "timestamp": now - timedelta(minutes=50),
        },
        {
            "movie_id": 1,
            "director_id": 1,
            "role_id": 1,
            "name": "Actor2",
            "cost": 1000,
            "timestamp": now - timedelta(minutes=40),
        },
        {
            "movie_id": 1,
            "director_id": 1,
            "role_id": 2,
            "name": "Actor3",
            "cost": 1000,
            "timestamp": now - timedelta(minutes=30),
        },
        {
            "movie_id": 2,
            "director_id": 1,
            "role_id": 1,
            "name": "Actor1",
            "cost": 1000,
            "timestamp": now - timedelta(minutes=20),
        },
        {
            "movie_id": 3,
            "director_id": 2,
            "role_id": 4,
            "name": "Actor56",
            "cost": 100000,
            "timestamp": now - timedelta(minutes=10),
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "MovieDS", df)

    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_complex_struct(client):
    """
    This tests the functionality where we don't have to specify a struct in
    includes of the extractor in various cases:
    1. Struct originates from input Featureset
    2. Struct originates from dataset
    3. Struct originates from extractor function
    """

    client.commit(
        message="msg",
        datasets=[MovieDS, MovieInfo],
        featuresets=[Request, MovieFeatures],
    )

    # Log data to test the pipeline
    _log_movie_data(client)

    client.sleep()

    # Extract the data
    input_df = pd.DataFrame(
        {
            "Request.movie_id": [1, 2, 3, 4],
            "Request.director_id": [1, 1, 2, 4],
            "Request.director_movie_id": [
                {"movie_id": 1, "director_id": 1},
                {"movie_id": 2, "director_id": 1},
                {"movie_id": 3, "director_id": 2},
                {"movie_id": 4, "director_id": 4},
            ],
        }
    )
    df = client.query(
        outputs=[
            MovieFeatures.role_list_py,
            MovieFeatures.role_list_assign,
            MovieFeatures.role_list_struct,
            MovieFeatures.movie_budget,
        ],
        inputs=[
            Request.movie_id,
            Request.director_id,
            Request.director_movie_id,
        ],
        input_dataframe=input_df,
    )

    assert df.shape[0] == 4
    assert len(df["MovieFeatures.role_list_py"].tolist()[0]) == 3
    assert df["MovieFeatures.role_list_py"].tolist()[0][0].as_json() == {
        "role_id": 2,
        "name": "Actor3",
        "cost": 1000,
    }
    assert df["MovieFeatures.role_list_py"].tolist()[0][1].as_json() == {
        "role_id": 1,
        "name": "Actor2",
        "cost": 1000,
    }
    assert df["MovieFeatures.role_list_py"].tolist()[0][2].as_json() == {
        "role_id": 1,
        "name": "Actor1",
        "cost": 1000,
    }
    assert len(df["MovieFeatures.role_list_py"].tolist()[1]) == 1
    assert df["MovieFeatures.role_list_py"].tolist()[1][0].as_json() == {
        "role_id": 1,
        "name": "Actor1",
        "cost": 1000,
    }
    assert len(df["MovieFeatures.role_list_py"].tolist()[2]) == 1
    assert df["MovieFeatures.role_list_py"].tolist()[2][0].as_json() == {
        "role_id": 4,
        "name": "Actor56",
        "cost": 100000,
    }
    assert len(df["MovieFeatures.role_list_py"].tolist()[3]) == 0

    assert (
        df["MovieFeatures.role_list_py"].tolist()
        == df["MovieFeatures.role_list_assign"].tolist()
    )
    assert (
        df["MovieFeatures.role_list_py"].tolist()
        == df["MovieFeatures.role_list_struct"].tolist()
    )

    assert len(df["MovieFeatures.movie_budget"].tolist()[0].roles) == 2
    assert df["MovieFeatures.movie_budget"].tolist()[0].roles[0].as_json() == {
        "role_id": 2,
        "count": 1,
        "total_cost": 1000,
    }
    assert df["MovieFeatures.movie_budget"].tolist()[0].roles[1].as_json() == {
        "role_id": 1,
        "count": 2,
        "total_cost": 2000,
    }

    assert len(df["MovieFeatures.movie_budget"].tolist()[1].roles) == 1
    assert df["MovieFeatures.movie_budget"].tolist()[1].roles[0].as_json() == {
        "role_id": 1,
        "count": 1,
        "total_cost": 1000,
    }

    assert len(df["MovieFeatures.movie_budget"].tolist()[2].roles) == 1
    assert df["MovieFeatures.movie_budget"].tolist()[2].roles[0].as_json() == {
        "role_id": 4,
        "count": 1,
        "total_cost": 100000,
    }

    assert len(df["MovieFeatures.movie_budget"].tolist()[3].roles) == 0
