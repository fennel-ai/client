import sys

sys.path.append('/Users/adityanambiar/fennel-ai/client')

from fennel.stream import (
    Stream,
    Postgres,
    source
)
from fennel.lib import (
    windows,
    Schema,
    Field,
)
from fennel.lib.schema import (
    Int,
    String,
    List,
)
from fennel.workspace import Workspace
import pandas as pd

# Source
# ----------------------------------------------------------------------
psql_src = Postgres(
    name='py_psql_src',
    host="my-favourite-postgres.us-west-2.rds.amazonaws.com",
    db_name="some_database_name",
    username="admin",
    password="password",
)


# Stream ---------------------------------------------------------------

class Actions(Stream):
    name = 'actions'
    retention = windows.DAY * 14
    stream: str

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', datatype=Int, default=0),
            Field('target_id', datatype=Int, default=0),
            Field('action_type', datatype=Int, default=0),
            Field('timestamp', datatype=Int, default='')
        )

    @source(psql_src, table='actions')
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


# Aggregate ------------------------------------------------------------

@env.pip_install('requests', '>=1.5')
class UserLikeCount(aggregate.Count):

    def __init__(self, stream, windows: List[Window]):
        self.stream = stream

    stream = Actions.name

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', int, 0, field_type=FieldType.Key, expectations=[expect_values_positive(1)]),
            Field('count', int, 0, field_type=FieldType.Value, expectations=[expect_values_positive(1)]),
            Field('timestamp', int, 0, field_type=FieldType.Timestamp, expectations=[expect_values_positive(1)]),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        df['actor_id'].rename('uid')
        df = df.drop('action_type')
        return df


# Feature -------------------------------------------------------------

@feature.pack(
    name='user_like_count',
    depends_on=[MyAgg],
    schema=Schema(
        Field('user_like_count_7days', Int),
        Field('user_like_count_28days', Int)
    ),
)
def user_like_count_3days(uids: pd.Series) -> pd.DataFrame:
    day7, day28 = aggregate.Lookup(MyAgg.name, uids=uids, window=[windows.Day, windows.Week])
    return pd.DataFrame({'user_like_count_1day': day7, 'user_like_count_7days': day28})


# Bring everything together -------------------------------------------------------------


if __name__ == '__main__':
    workspace = Workspace(name='quick_workflow', url='http://localhost:8080')
    workspace.register_streams(Actions())
    workspace.register_aggregates(UserLikeCount)
    workspace.register(user_like_count_3days)
    workspace.extract(feature_names, df, config)
