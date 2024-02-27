import unittest
from unittest.mock import patch

import requests

SERVER = "http://localhost:8000/"
BRANCH_NAME = "main"


class TestRestAPI(unittest.TestCase):
    @patch("requests.post")
    def test_log(self, mock_post):
        mock_post.return_value.status_code = 200
        # docsnip rest_log_api
        url = "{}/api/v1/log".format(SERVER)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer <API-TOKEN>",
        }
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
        req = {
            "webhook": "fennel_webhook",
            "endpoint": "UserInfo",
            "data": data,
        }
        response = requests.post(url, headers=headers, data=req)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip

        # docsnip rest_extract_api
        url = "{}/api/v1/branch/{}/query".format(SERVER, BRANCH_NAME)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer <API-TOKEN>",
        }
        data = [
            {"UserFeatures.userid": 1},
            {"UserFeatures.userid": 2},
            {"UserFeatures.userid": 3},
        ]
        req = {
            "outputs": ["UserFeatures"],
            "inputs": ["UserFeatures.userid"],
            "data": data,
            "log": True,
            "workflow": "test",
        }

        response = requests.post(url, headers=headers, data=req)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip

        # docsnip rest_extract_api_columnar
        url = "{}/api/v1/branch/{}/query".format(SERVER, BRANCH_NAME)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer <API-TOKEN>",
        }
        data = {"UserFeatures.userid": [1, 2, 3]}
        req = {
            "outputs": ["UserFeatures"],
            "inputs": ["UserFeatures.userid"],
            "data": data,
            "log": True,
            "workflow": "test",
        }

        response = requests.post(url, headers=headers, data=req)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip
