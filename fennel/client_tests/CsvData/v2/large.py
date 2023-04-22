import pandas as pd
import copy
from datetime import datetime
from random import randrange


_START = datetime.fromisoformat('2022-01-01')
_END = datetime.today()

def rand_datetime() -> datetime:
    """Returns a random datetime between start and end."""

    return datetime.fromtimestamp(randrange(
        round(_START.timestamp()), round(_END.timestamp())
    ))


def main():
    shortcutActivityDf = pd.read_csv("/Users/mohitreddy/fennel-ai/client/fennel/client_tests/CsvData/v2/shortcutActivity.csv")
    rows = []
    for row in shortcutActivityDf.itertuples(index=False):
        for _ in range(100):
            r = copy.copy(row)
            r = r._replace(time=rand_datetime())
            rows.append(r)
    df = pd.DataFrame(rows, columns=shortcutActivityDf.columns)
    print(df)
    df.to_csv("/Users/mohitreddy/fennel-ai/client/fennel/client_tests/CsvData/v2/shortcutActivityLarge.csv", index=False)

if __name__ == "__main__":
    main()