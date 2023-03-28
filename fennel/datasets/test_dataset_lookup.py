import inspect
from datetime import datetime
from textwrap import dedent
from typing import List, no_type_check

import pandas as pd

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.metadata import meta
from fennel.lib.schema import DataFrame, Series
from fennel.test_lib import *


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str = field(key=True)
    gender: str
    # Users date of birth
    dob: str
    age: int
    timestamp: datetime


def dataset_lookup(
    cls_name: str, ts: pd.Series, fields: List[str], df: pd.DataFrame
):
    now = datetime.fromtimestamp(1668368655)
    if len(fields) > 0:
        assert ts.equals(pd.Series([now, now, now]))
        assert fields == ["age", "gender"]
        assert df["user_id"].tolist() == [5, 10, 15]
        assert df["name"].tolist() == ["a", "b", "c"]
        lst = [[24, "female"], [23, "female"], [45, "male"]]
        df = pd.DataFrame(lst, columns=fields)
        return df, pd.Series([True, True, True])
    else:
        assert ts.equals(pd.Series([now, now, now]))
        assert fields == []
        assert df["user_id"].tolist() == [3, 6, 9]
        assert df["name"].tolist() == ["a2", "b2", "c2"]
        lst = [[24], [23], [45]]
        df = pd.DataFrame(lst, columns=["age"])
        return df, pd.Series([True, True, True])


def test_dataset_lookup(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class UserAgeFeatures:
        userid: int = feature(id=1)
        name: str = feature(id=2)
        # The users gender among male/female/non-binary
        age_sq: int = feature(id=3).meta(owner="aditya@fennel.ai")
        age_cube: int = feature(id=4).meta(owner="mohit@fennel.ai")
        gender: str = feature(id=5)

        @extractor
        @depends_on(UserInfoDataset)
        @no_type_check
        def user_age_sq(
            cls,
            ts: Series[datetime],
            user_id: Series[userid],
            names: Series[name],
        ) -> DataFrame[age_sq, gender]:
            user_id_plus_one = user_id * 5
            df, _ = UserInfoDataset.lookup(
                ts,
                user_id=user_id_plus_one,
                name=names,
                fields=["age", "gender"],
            )
            df["age_sq"] = df["age"] * df["age"]
            return df[["age_sq", "gender"]]

        @extractor
        @depends_on(UserInfoDataset)
        @no_type_check
        def user_age_cube(
            cls,
            ts: Series[datetime],
            user_id: Series[userid],
            names: Series[name],
        ) -> Series[age_cube]:
            user_id_into_three = user_id * 3
            df, _ = UserInfoDataset.lookup(
                ts,
                user_id=user_id_into_three,
                name=names,
            )
            df["age_cube"] = df["age"] * df["age"] * df["age"]
            return df[["age_cube"]]

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserAgeFeatures)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 1
    user_sq_extractor = sync_request.extractors[1]
    assert user_sq_extractor.name == "user_age_sq"

    dscode_dict = {
        "UserInfoDataset": dedent(
            inspect.getsource(UserInfoDataset.__fennel_original_cls__)
        )
    }
    # Call to the extractor function

    user_sq_extractor_func, globals, locals = get_extractor_func(
        sync_request.extractors[1],
        {"UserInfoDataset": UserInfoDataset},
        dscode_dict,
        "",
    )

    now = datetime.fromtimestamp(1668368655)
    ts = pd.Series([now, now, now])
    user_id = pd.Series([1, 2, 3])
    names = pd.Series(["a", "b", "c"])

    globals["dataset_lookup"] = dataset_lookup
    df = user_sq_extractor_func(None, ts, user_id, names)
    assert df["age_sq"].tolist() == [576, 529, 2025]
    assert df["gender"].tolist() == ["female", "female", "male"]

    user_age_cube = sync_request.extractors[0]
    assert user_age_cube.name == "user_age_cube"

    # Call to the extractor function
    user_age_cube_func, globals, locals = get_extractor_func(
        sync_request.extractors[0],
        {"UserInfoDataset": UserInfoDataset},
        dscode_dict,
        "",
    )

    ts = pd.Series([now, now, now])
    user_id = pd.Series([1, 2, 3])
    names = pd.Series(["a2", "b2", "c2"])
    globals["dataset_lookup"] = dataset_lookup
    df = user_age_cube_func(None, ts, user_id, names)
    assert df["age_cube"].tolist() == [13824, 12167, 91125]
