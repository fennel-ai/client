from locust import task
from locust_utils import FennelLocustUser, CSVReader

user_reader = CSVReader("benchmark/data/user_data.csv")
posts_reader = CSVReader("benchmark/data/post_data.csv")

NUM_CANDIDATES = 100


def get_val(x):
    return x[0]


class FennelWorker(FennelLocustUser):
    #
    # @task(1)
    # def get_query_value(self):
    #     self.client.get_features(user_id_1=get_val(next(user_reader)),
    #         user_id_2=get_val(next(user_reader)),
    #         specifier='extract_features')

    # @task(1)
    # def get_single_feature(self):
    #     self.client.get_single_ds_lookup(
    #         user_ids=[
    #             get_val(next(user_reader)) for _ in range(NUM_CANDIDATES)
    #         ],
    #         specifier=f"Single {NUM_CANDIDATES} candidates",
    #     )

    @task(1)
    def get_random_features(self):
        self.client.get_random_features(
            user_ids=[
                get_val(next(user_reader)) for _ in range(NUM_CANDIDATES)
            ],
            specifier=f"Random {NUM_CANDIDATES} candidates",
        )
    #
    # @task(1)
    # def get_multiple_features(self):
    #     self.client.get_complex_ds_lookup(
    #         user_ids=[
    #             get_val(next(user_reader)) for _ in range(NUM_CANDIDATES)
    #         ],
    #         specifier=f"Complex {NUM_CANDIDATES} candidates",
    #     )

    # @task(1)
    # def get_100_features(self):
    #     self.client.get_100_features(
    #         user_ids=[
    #             get_val(next(user_reader)) for _ in range(NUM_CANDIDATES)
    #         ],
    #         specifier=f"Complex {NUM_CANDIDATES} candidates",
    #     )
