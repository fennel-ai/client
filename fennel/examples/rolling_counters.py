import pandas as pd

import aggregate
import config
import connector
from expectations import *
import feature
from schema import *
import stream
import windows

configs = []


@pip_install([('pandas', '==1.5', '<2.0'), 'numpy', ])
class Actions(stream.Stream):
    name = 'actions'
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', datatype=Int, default=0, expectations=[expect_values_positive(1)]),
            Field('target_id', datatype=Int, default=0),
            Field('action_type', datatype=Int, default=0),
            Field('timestamp', datatype=Int, default='')
        )

    @connector.mysql('localhost', 'admin', 'password', 'actions', mode='pandas')
    def populate_from_mysql(self, df: pd.DataFrame) -> pd.DataFrame:
        # imagine the format of date is: 'YYYY-MM-DD:HH:MM:SS'
        df['timestamp'] = pd.to_datetime(df['date'])
        df.drop('useless_column')
        return df


@pip_install('requests', '>=1.5')
class UserLikeCount(aggregate.Count):
    windows = [windows.DAY, 3 * windows.DAY]
    stream = Actions.name

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', int, 0, type_=FieldType.Key, expectations=[expect_values_positive(1)]),
            Field('count', int, 0, type_=FieldType.Value, expectations=[expect_values_positive(1)]),
            Field('timestamp', int, 0, type_=FieldType.Timestamp, expectations=[expect_values_positive(1)]),
        )

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        df['actor_id'].rename('uid')
        df = df.drop('action_type')
        return df


# TODO(nikhil): think about more words for "feature.pack"
@feature.pack(
    name='user_like_count',
    mode='pandas', depends_on_aggregates=[UserLikeCount.name], depends_on_features=['my_feature'],
    schema=Schema(
        Field('3days', Int),  # user_like_count:3days
        Field('30days', Int)  # user_like_count:30days
    ),
)
@pip_install('requests', '>=1.5')
@config.depends_on('min_value_user_like_count_day3', 'max_value_user_like_count_day30')
@config.depends_on('min_value_user_like_count_day30', 'max_value_user_like_count_day30')
def user_like_count_3days(uids: pd.Series, categories: pd.Series) -> pd.DataFrame:
    day3 = UserLikeCount.lookup(uids=uids, categories=categories, window=windows.DAY * 3)
    day30 = UserLikeCount.lookup(uids=uids, categories=categories, window=windows.DAY * 3)
    min_value = config.read('min_values')
    max_value = config.read('max_values')
    tempvar = user_like_count_3days.extract(df['a'], df['b'])
    if min_value is not None:
        day3 = day3.apply(lambda x: (max_value - x) / (max_value - min_value))
        day30 = day30.apply(lambda x: (max_value - x) / (max_value - min_value))
    return pd.DataFrame({'user_like_count_3days': day3, 'user_like_count_30days': day30})


def main():
    categories = pd.Series([1, 2, 3])
    feautre_names = []
    df = pd.DataFrame()
    uids = request.logged_in_user()
    uids = pd.Series([uid])
    os = pd.Series([request.current_os()])
    config = configerator.get_current()
    return feature.extract_magic(feautre_names, df, config={'a': 1, 'b': 2}, batch=False, concact=True)
