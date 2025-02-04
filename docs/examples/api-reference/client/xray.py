from fennel.testing import mock


@mock
def test_basic(client):
    def xray_client():
        # docsnip xray
        client.xray(
            datasets=["Dataset1", "Dataset2"],
            featuresets=[],
            properties=["backlog", "sink_destination", "name"]
        )  # docsnip-highlight
        # /docsnip
