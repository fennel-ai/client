from locust import task, between
from locust_utils import FennelLocustUser, CSVReader

user_reader = CSVReader("benchmark/data/user_data.csv")
posts_reader = CSVReader("benchmark/data/post_data.csv")


def get_val(x):
    return x[0]


class FennelWorker(FennelLocustUser):
    wait_time = between(1, 2)

    @task(1)
    def get_query_value(self):
        self.client.get_features(user_id_1=get_val(next(user_reader)),
            user_id_2=get_val(next(user_reader)),
            specifier='extract_features')
