import pandas as pd
import pytest

from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature as F, extractor
from fennel.lib import inputs, outputs
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


def test_invalid_dataset_lookup():
    """
    This tests that a dataset without both offline and online index cannot be added to depends_on of an extractor.
    """

    @dataset
    class Dataset1:
        user_id: int = field(key=True)
        ts: int = field(timestamp=True)
        age: int

    @dataset
    class Dataset2:
        user_id: int = field(key=True)
        ts: int = field(timestamp=True)
        age: int

    with pytest.raises(ValueError) as e:

        @featureset
        class Featureset1:
            user_id: int
            age: int = F(Dataset1.age, default=10)

    assert (
        str(e.value)
        == "Please define either an offline or online index on dataset : Dataset1 for extractor to work."
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class Featureset2:
            user_id: int
            age: int = F(Dataset2.age, default=10)

    assert (
        str(e.value)
        == "Please define either an offline or online index on dataset : Dataset2 for extractor to work."
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class Featureset3:
            user_id: int
            age: int

            @inputs("user_id")
            @outputs("age")
            @extractor(deps=[Dataset1])
            def extract(cls, ts: pd.Series, user_ids: pd.Series):
                data, _ = Dataset1.lookup(ts, user_id=user_ids)  # type: ignore
                return data["age"].fillna(10)

    assert (
        str(e.value)
        == "Please define either an offline or online index on dataset : Dataset1 for extractor to work."
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class Featureset4:
            user_id: int
            age: int

            @inputs("user_id")
            @outputs("age")
            @extractor(deps=[Dataset2])
            def extract(cls, ts: pd.Series, user_ids: pd.Series):
                data, _ = Dataset2.lookup(ts, user_id=user_ids)  # type: ignore
                return data["age"].fillna(10)

    assert (
        str(e.value)
        == "Please define either an offline or online index on dataset : Dataset2 for extractor to work."
    )


@mock
def test_invalid_dataset_online_lookup(client):
    """
    This tests that a dataset doing online lookup without online index is not allowed.
    """

    @source(webhook.endpoint("Dataset1"), disorder="14d", cdc="upsert")
    @dataset(offline=True)
    class Dataset1:
        user_id: int = field(key=True)
        ts: int = field(timestamp=True)
        age: int

    @featureset
    class Featureset1:
        user_id: int
        age: int = F(Dataset1.age, default=10)

    @featureset
    class Featureset2:
        user_id: int
        age: int

        @extractor(deps=[Dataset1])
        @inputs("user_id")
        @outputs("age")
        def extract(cls, ts: pd.Series, user_ids: pd.Series):
            data, _ = Dataset1.lookup(ts, user_id=user_ids)  # type: ignore
            return data["age"].fillna(10)

    client.commit(
        message="tst",
        datasets=[Dataset1],
        featuresets=[Featureset1, Featureset2],
    )

    with pytest.raises(ValueError) as e:
        client.query(
            inputs=[Featureset1.user_id],
            outputs=[Featureset1.age],
            input_dataframe=pd.DataFrame({"Featureset1.user_id": [1, 2, 3, 4]}),
        )
    assert str(e.value) == "Please define an online index on dataset : Dataset1"

    with pytest.raises(Exception) as e:
        client.query(
            inputs=[Featureset2.user_id],
            outputs=[Featureset2.age],
            input_dataframe=pd.DataFrame({"Featureset2.user_id": [1, 2, 3, 4]}),
        )
    assert (
        str(e.value)
        == "Extractor `extract` in `Featureset2` failed to run with error: Please define an online index on dataset : Dataset1. "
    )


@mock
def test_invalid_dataset_offline_lookup(client):
    """
    This tests that a dataset doing offline lookup without offline index is not allowed.
    """

    @source(webhook.endpoint("Dataset1"), disorder="14d", cdc="upsert")
    @dataset(online=True)
    class Dataset1:
        user_id: int = field(key=True)
        ts: int = field(timestamp=True)
        age: int

    @featureset
    class Featureset1:
        user_id: int
        age: int = F(Dataset1.age, default=10)

    @featureset
    class Featureset2:
        user_id: int
        age: int

        @extractor(deps=[Dataset1])
        @inputs("user_id")
        @outputs("age")
        def extract(cls, ts: pd.Series, user_ids: pd.Series):
            data, _ = Dataset1.lookup(ts, user_id=user_ids)  # type: ignore
            return data["age"].fillna(10)

    client.commit(
        message="tst",
        datasets=[Dataset1],
        featuresets=[Featureset1, Featureset2],
    )

    with pytest.raises(ValueError) as e:
        client.query_offline(
            inputs=[Featureset1.user_id],
            outputs=[Featureset1.age],
            input_dataframe=pd.DataFrame(
                {"Featureset1.user_id": [1, 2, 3, 4], "ts": [1, 1, 1, 1]}
            ),
            timestamp_column="ts",
        )
    assert (
        str(e.value) == "Please define an offline index on dataset : Dataset1"
    )

    with pytest.raises(Exception) as e:
        client.query_offline(
            inputs=[Featureset2.user_id],
            outputs=[Featureset2.age],
            input_dataframe=pd.DataFrame(
                {"Featureset2.user_id": [1, 2, 3, 4], "ts": [1, 1, 1, 1]}
            ),
            timestamp_column="ts",
        )
    assert (
        str(e.value)
        == "Extractor `extract` in `Featureset2` failed to run with error: Please define an offline index on dataset : Dataset1. "
    )
