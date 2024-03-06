from datetime import datetime
from typing import List

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import sources
from fennel.datasets import dataset, Dataset, pipeline, field, LastK
from fennel.featuresets import featureset, feature, extractor
from fennel.lib import meta, inputs, outputs
from fennel.dtypes import struct
from fennel.sources import source
from fennel.testing import mock

webhook = sources.Webhook(name="fennel_webhook")


@struct
class Movie:
    movie_id: int
    title: str


@struct
class Cast:
    name: str
    actor_id: int
    age: int


@meta(owner="test@test.com")
@source(webhook.endpoint("MovieInfo"), disorder="14d", cdc="append")
@dataset
class MovieCast:
    movie: Movie
    cast: Cast
    timestamp: datetime = field(timestamp=True)


@meta(owner="test@test.com")
@dataset
class MovieInfo:
    movie: Movie = field(key=True)
    cast_list: List[Cast]
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(MovieCast)
    def movie_info(cls, movie_cast: Dataset):
        return movie_cast.groupby("movie").aggregate(
            [
                LastK(
                    into_field="cast_list",
                    of="cast",
                    window="forever",
                    limit=3,
                    dedup=False,
                ),
            ]
        )


@meta(owner="test@test.com")
@featureset
class MovieFeatures:
    movie: Movie = feature(id=1)
    cast_list: List[Cast] = feature(id=2)
    average_cast_age: float = feature(id=3)

    @extractor(depends_on=[MovieInfo])
    @inputs(movie)
    @outputs(cast_list)
    def extract_cast(cls, ts: pd.Series, movie: pd.Series):
        res, _ = MovieInfo.lookup(ts, movie=movie)  # type: ignore
        return pd.Series(res["cast_list"])

    @extractor(depends_on=[MovieInfo])
    @inputs(movie)
    @outputs(average_cast_age)
    def extract_average_cast_age(cls, ts: pd.Series, movie: pd.Series):
        res, _ = MovieInfo.lookup(ts, movie=movie)  # type: ignore
        res["total_cast_age"] = res["cast_list"].apply(
            lambda x: sum([c.age for c in x])
        )
        res["average_cast_age"] = res["total_cast_age"] / res[
            "cast_list"
        ].apply(lambda x: len(x))
        return pd.Series(res["average_cast_age"])


def log_movie_data(client):
    now = datetime.utcnow()
    data = [
        [
            {"movie_id": 101, "title": "Inception"},
            {"name": "Leonardo DiCaprio", "actor_id": 1, "age": 46},
            now,
        ],
        [
            {"movie_id": 101, "title": "Inception"},
            {"name": "Ellen Page", "actor_id": 2, "age": 34},
            now - pd.Timedelta("1 day"),
        ],
        [
            {"movie_id": 102, "title": "Titanic"},
            {"name": "Leonardo DiCaprio", "actor_id": 1, "age": 46},
            now,
        ],
        [
            {"movie_id": 102, "title": "Titanic"},
            {"name": "Kate Winslet", "actor_id": 3, "age": 45},
            now - pd.Timedelta("1 day"),
        ],
    ]
    columns = ["movie", "cast", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    response = client.log("fennel_webhook", "MovieInfo", df)

    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_struct_type(client):
    client.commit(
        message="Initial commit",
        datasets=[MovieCast, MovieInfo],
        featuresets=[MovieFeatures],
    )
    # Log data to test the pipeline
    log_movie_data(client)

    client.sleep()

    input_df = pd.DataFrame(
        {
            "MovieFeatures.movie": [
                {"movie_id": 101, "title": "Inception"},
                {"movie_id": 102, "title": "Titanic"},
            ],
        }
    )
    df = client.query(
        outputs=[
            MovieFeatures.cast_list,
            MovieFeatures.average_cast_age,
        ],
        inputs=[MovieFeatures.movie],
        input_dataframe=input_df,
    )

    # Verify the returned dataframe
    assert df.shape == (2, 2)
    assert df.columns.tolist() == [
        "MovieFeatures.cast_list",
        "MovieFeatures.average_cast_age",
    ]
    assert (
        len(df["MovieFeatures.cast_list"][0]) == 2
    )  # 2 cast members for "Inception"
    assert len(df["MovieFeatures.cast_list"][1]) == 2  # 2 cast members for
    # "Titanic"
    leanardo = Cast(name="Leonardo DiCaprio", actor_id=1, age=46)
    cast1 = df["MovieFeatures.cast_list"][0][0]
    assert cast1.name == leanardo.name
    assert cast1.actor_id == leanardo.actor_id
    assert cast1.age == leanardo.age

    cast2 = df["MovieFeatures.cast_list"][0][1]
    ellen = Cast(name="Ellen Page", actor_id=2, age=34)
    assert cast2.name == ellen.name
    assert cast2.actor_id == ellen.actor_id
    assert cast2.age == ellen.age

    cast3 = df["MovieFeatures.cast_list"][1][0]
    assert cast3.name == leanardo.name
    assert cast3.actor_id == leanardo.actor_id
    assert cast3.age == leanardo.age

    cast4 = df["MovieFeatures.cast_list"][1][1]
    kate = Cast(name="Kate Winslet", actor_id=3, age=45)
    assert cast4.name == kate.name
    assert cast4.actor_id == kate.actor_id
    assert cast4.age == kate.age

    # Test extract_average_cast_age extractor

    assert df["MovieFeatures.average_cast_age"][0] == 40
    assert df["MovieFeatures.average_cast_age"][1] == 45.5
