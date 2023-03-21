import unittest
from unittest.mock import patch

import requests

url = "http://localhost:8000/api/v1/"


class TestRestAPI(unittest.TestCase):
    @patch("requests.post")
    def test_log(self, mock_post):
        mock_post.return_value.status_code = 200
        # docsnip rest_log_api
        headers = {"Content-Type": "application/json"}
        data = [
            {
                "user_id": 1,
                "name": "John",
                "age": 20,
                "country": "Russia",
                "timestamp": "2020-01-01",
            },
            {
                "user_id": 2,
                "name": "Monica",
                "age": 24,
                "country": "Chile",
                "timestamp": "2021-03-01",
            },
            {
                "user_id": 3,
                "name": "Bob",
                "age": 32,
                "country": "USA",
                "timestamp": "2020-01-01",
            },
        ]
        req = {"dataset": "UserInfo", "rows": data}
        response = requests.post(url, headers=headers, data=req)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip

        # docsnip rest_extract_features_api
        headers = {"Content-Type": "application/json"}
        data = [
            {"UserFeatures.userid": 1},
            {"UserFeatures.userid": 2},
            {"UserFeatures.userid": 3},
        ]
        req = {
            "output_features": ["UserFeatures"],
            "input_features": ["UserFeatures.userid"],
            "data": data,
            "log": True,
            "workflow": "test",
        }

        response = requests.post(url, headers=headers, data=req)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip
