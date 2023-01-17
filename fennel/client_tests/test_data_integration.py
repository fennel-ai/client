import time
import unittest
from datetime import datetime

import pandas as pd
import pytest
import requests

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.sources import source, S3
from fennel.test_lib import mock_client

s3 = S3(
    name="ratings_source",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
)


@meta(owner="xiao@fennel.ai")
@source(
    s3.bucket(
        bucket_name="fennel-demo-data",
        prefix="movielens_sampled/movies_timestamped.csv",
    ),
    every="1h",
)
@dataset
class MovieInfo:
    movieId: int = field(key=True).meta(description="Movie ID")  # type: ignore
    title: str = field().meta(  # type: ignore
        description="Title along with year"
    )
    genres: str
    timestamp: datetime = field(timestamp=True)


class TestMovieInfo(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_log_to_movieinfo(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # Sync the dataset
        client.sync(datasets=[MovieInfo])
        t = datetime.fromtimestamp(1672858163)
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
        response = client.log("MovieInfo", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do some lookups
        now = datetime.now()
        movie_ids = pd.Series([1, 2, 23, 123343])
        ts = pd.Series([now, now, now, now])
        df, found = MovieInfo.lookup(
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
        past = datetime.fromtimestamp(1672858160)
        ts = pd.Series([past, past, now, past])
        df, found = MovieInfo.lookup(
            ts,
            movieId=movie_ids,
        )
        assert found.tolist() == [False, False, True, False]

    @pytest.mark.data_integration
    @mock_client
    def test_s3_data_integration_source(self, client):
        """Same test as test_log_to_movieinfo but with an S3 source."""
        # Sync the dataset
        client.sync(datasets=[MovieInfo])

        # Time for data_integration to do its magic
        time.sleep(10)

        # Do some lookups
        now = datetime.now()
        movie_ids = pd.Series([1, 2, 23, 123343])
        ts = pd.Series([now, now, now, now])
        df, found = MovieInfo.lookup(
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
        past = datetime.fromtimestamp(1672858160)
        ts = pd.Series([past, past, now, past])
        df, found = MovieInfo.lookup(
            ts,
            movieId=movie_ids,
        )
        assert found.tolist() == [False, False, True, False]
