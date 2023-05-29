from datetime import datetime

# docsnip expectations
from fennel.datasets import dataset
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
    expect_column_values_to_be_in_set,
    expect_column_pair_values_A_to_be_greater_than_B,
)
from fennel.lib.schema import between


@dataset
class Sample:
    passenger_count: between(int, 0, 100)
    gender: str
    age: between(int, 0, 100, strict_min=True)
    mothers_age: between(int, 0, 100, strict_min=True)
    timestamp: datetime

    @expectations
    def my_function(cls):
        return [
            expect_column_values_to_be_between(
                column=str(cls.passenger_count),
                min_value=1,
                max_value=6,
                mostly=0.95,
            ),
            expect_column_values_to_be_in_set(str(cls.gender), ["male", "female"], mostly=0.99),
            # Pairwise expectation
            expect_column_pair_values_A_to_be_greater_than_B(column_A=str(cls.age), column_B=str(cls.mothers_age)),
        ]


# /docsnip
