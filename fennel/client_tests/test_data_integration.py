import time
import unittest
from datetime import datetime, timezone

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.connectors import source, S3, Webhook
from fennel.datasets import dataset, field
from fennel.lib import meta
from fennel.testing import mock

s3 = S3(
    name="ratings_source",
    aws_access_key_id="AKIAQOLFGTNXKQAT3UF5",
    aws_secret_access_key="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",
)

webhook = Webhook(name="fennel_webhook")


@meta(owner="xiao@fennel.ai")
@source(webhook.endpoint("MovieInfo"), disorder="14d", cdc="upsert", env="dev")
@source(
    s3.bucket(
        bucket_name="fennel-demo-data",
        prefix="movielens_sampled/movies_timestamped.csv",
    ),
    every="1h",
    disorder="14d",
    cdc="upsert",
    env="prod",
)
@dataset(index=True)
class MovieInfo103:
    movieId: int = field(key=True).meta(description="Movie ID")  # type: ignore
    title: str = field().meta(  # type: ignore
        description="Title along with year"
    )
    genres: str
    timestamp: datetime = field(timestamp=True)


class TestMovieInfo103(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_log_to_MovieInfo103(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # Sync the dataset
        client.commit(message="msg", datasets=[MovieInfo103], env="dev")
        t = datetime.fromtimestamp(1672858163, tz=timezone.utc)
        data = [
            [
                1,
                "Toy Story (1995)",
                "Adventure|Animation|Children|Comedy|Fantasy",
                t,
            ],
            [2, "Jumanji (1995)", "Adventure|Children|Fantasy", t],
            [23, "Assassins (1995)", "Action|Crime|Thriller", t],
            [24, "Powder (1995)", "Drama|Sci-Fi", t],
        ]
        columns = ["movieId", "title", "genres", "timestamp"]
        df = pd.DataFrame(data, columns=columns)

        response = client.log("fennel_webhook", "MovieInfo", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        # Do some lookups
        now = datetime.now(timezone.utc)
        keys = pd.DataFrame({"movieId": [1, 2, 23, 123343]})
        df, found = client.lookup(
            "MovieInfo103",
            keys=keys,
        )
        assert found.tolist() == [True, True, True, False]
        assert df["title"].tolist() == [
            "Toy Story (1995)",
            "Jumanji (1995)",
            "Assassins (1995)",
            pd.NA,
        ]
        assert df["genres"].tolist() == [
            "Adventure|Animation|Children|Comedy|Fantasy",
            "Adventure|Children|Fantasy",
            "Action|Crime|Thriller",
            pd.NA,
        ]

        # Do some lookups with a timestamp
        past = datetime.fromtimestamp(1672858160, tz=timezone.utc)
        ts = pd.Series([past, past, now, past])
        df, found = client.lookup(
            "MovieInfo103",
            timestamps=ts,
            keys=keys,
        )
        assert found.tolist() == [False, False, True, False]

    @pytest.mark.data_integration
    @mock
    def test_s3_data_integration_source(self, client):
        """Same test as test_log_to_MovieInfo103 but with an S3 source."""
        # Sync the dataset
        client.commit(message="msg", datasets=[MovieInfo103], env="prod")
        client.sleep()

        # Time for data_integration to do its magic
        time.sleep(10)

        # Do some lookups
        now = datetime.now(timezone.utc)
        movie_ids = pd.Series([1, 2, 23, 123343])
        ts = pd.Series([now, now, now, now])
        df, found = MovieInfo103.lookup(
            ts,
            movieId=movie_ids,
        )
        assert found.tolist() == [True, True, True, False]
        assert df["title"].tolist() == [
            "Toy Story (1995)",
            "Jumanji (1995)",
            "Assassins (1995)",
            None,
        ]
        assert df["genres"].tolist() == [
            "Adventure|Animation|Children|Comedy|Fantasy",
            "Adventure|Children|Fantasy",
            "Action|Crime|Thriller",
            None,
        ]

        # Do some lookups with a timestamp
        past = datetime.fromtimestamp(1672858160, tz=timezone.utc)
        ts = pd.Series([past, past, now, past])
        df, found = MovieInfo103.lookup(
            ts,
            movieId=movie_ids,
        )
        assert found.tolist() == [False, False, True, False]

    @mock
    def test_epoch_log_to_MovieInfo103(self, client):
        """Log some data to the dataset with epoch time and check if it is logged correctly."""
        # Sync the dataset
        client.commit(message="msg", datasets=[MovieInfo103], env="dev")
        data = [
            [
                1,
                "Toy Story (1995)",
                "Adventure|Animation|Children|Comedy|Fantasy",
                1672858163,
            ],
            [2, "Jumanji (1995)", "Adventure|Children|Fantasy", 1672858163000],
            [23, "Assassins (1995)", "Action|Crime|Thriller", 1672858163000000],
            [
                24,
                "Powder (1995)",
                "Drama|Sci-Fi",
                datetime.fromtimestamp(1672858163, tz=timezone.utc),
            ],
        ]
        columns = ["movieId", "title", "genres", "timestamp"]
        df = pd.DataFrame(data, columns=columns)

        response = client.log("fennel_webhook", "MovieInfo", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        # Dataset name is the class name
        df = client.get_dataset_df("MovieInfo103")
        assert df["title"].tolist() == [
            "Toy Story (1995)",
            "Jumanji (1995)",
            "Assassins (1995)",
            "Powder (1995)",
        ]
        assert df["timestamp"].tolist() == [
            datetime.fromtimestamp(1672858163, tz=timezone.utc),
            datetime.fromtimestamp(1672858163, tz=timezone.utc),
            datetime.fromtimestamp(1672858163, tz=timezone.utc),
            datetime.fromtimestamp(1672858163, tz=timezone.utc),
        ]
