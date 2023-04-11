import csv
import random
import time

import pandas as pd
from typing import List

from benchmark.copy_features import UserCopyFeatures
from benchmark.social_network import Request, UserFeatures, UserRandomFeatures
from fennel.test_lib import LocalClient
from locust import User, events

MS_IN_SECOND = 1000
EXTRACT_FEATURE = "extract_feature"
CATEGORIES = [
    "graphic",
    "Craft",
    "politics",
    "political",
    "Mathematics",
    "zoology",
    "business",
    "dance",
    "banking",
    "HR management",
    "art",
    "science",
    "Music",
    "operating system",
    "Fashion Design",
    "programming",
    "painting",
    "photography",
    "drawing",
    "GST",
]


class CSVReader:
    "Read test data from csv file using an iterator"

    def __init__(self, file):
        try:
            file = open(file)
        except TypeError:
            pass
        self.file = file
        self.reader = csv.reader(file)

    def __next__(self):
        try:
            return next(self.reader)
        except StopIteration:
            # reuse file on EOF
            self.file.seek(0, 0)
            return next(self.reader)


class FennelLocusClient(object):
    def __init__(self, host):
        self.client = LocalClient("http://localhost:23104",
            "http://localhost:23103", None)

    def _get_result_len(self, result):
        if isinstance(result, pd.DataFrame):
            return len(result)
        elif isinstance(result, pd.Series):
            return len(result)
        else:
            return 0

    def _get_elapsed_time(self, start_time):
        return int((time.time() - start_time) * MS_IN_SECOND)

    def get_single_ds_lookup(self, user_ids: List[str], specifier):
        start_time = time.time()
        failure = True
        err = ""
        try:
            # Pick a random category
            result = self.client.extract_features(
                output_feature_list=[UserFeatures.num_views],
                input_feature_list=[Request.user_id],
                input_dataframe=pd.DataFrame(
                    {
                        "Request.user_id": user_ids,
                    }
                ),
            )
            if result.shape[0] == len(user_ids):
                failure = False
            else:
                err = "Result shape is not {}, it is {}".format(
                    len(user_ids), result.shape
                )
        except Exception as e:
            err = str(e)
            failure = True

        if failure:
            total_time = self._get_elapsed_time(start_time)
            events.request.fire(
                request_type=EXTRACT_FEATURE,
                name=specifier,
                response_time=total_time,
                exception=err,
                response_length=0,
            )
            return -1

        total_time = int((time.time() - start_time) * MS_IN_SECOND)
        events.request.fire(
            request_type=EXTRACT_FEATURE,
            name=specifier,
            response_time=total_time,
            exception=None,
            response_length=self._get_result_len(result),
        )
        return result

    def get_complex_ds_lookup(self, user_ids: List[str], specifier):
        start_time = time.time()
        failure = True
        err = ""
        try:
            # Pick a random category
            catogories = [
                random.choice(CATEGORIES) for _ in range(len(user_ids))
            ]
            result = self.client.extract_features(
                output_feature_list=[UserFeatures],
                input_feature_list=[Request],
                input_dataframe=pd.DataFrame(
                    {
                        "Request.user_id": user_ids,
                        "Request.category": catogories,
                    }
                ),
            )
            if result.shape == (len(user_ids), 3):
                failure = False
            else:
                err = "Result shape is not {}x3, it is {}".format(
                    len(user_ids), result.shape
                )
        except Exception as e:
            err = str(e)
            failure = True

        if failure:
            total_time = self._get_elapsed_time(start_time)
            events.request.fire(
                request_type=EXTRACT_FEATURE,
                name=specifier,
                response_time=total_time,
                exception=err,
                response_length=0,
            )
            return -1

        total_time = int((time.time() - start_time) * MS_IN_SECOND)
        events.request.fire(
            request_type=EXTRACT_FEATURE,
            name=specifier,
            response_time=total_time,
            exception=None,
            response_length=self._get_result_len(result),
        )
        return result

    def get_random_features(self, user_ids: List[str], specifier):
        start_time = time.time()
        failure = True
        err = ""
        try:
            # Pick a random category
            result = self.client.extract_features(
                output_feature_list=[UserRandomFeatures],
                input_feature_list=[Request.user_id],
                input_dataframe=pd.DataFrame({"Request.user_id": user_ids}),
            )
            if result.shape == (len(user_ids), 2):
                failure = False
            else:
                err = "Result shape is not {}x2, it is {}".format(
                    len(user_ids), result.shape
                )
        except Exception as e:
            err = str(e)
            failure = True

        if failure:
            total_time = self._get_elapsed_time(start_time)
            events.request.fire(
                request_type=EXTRACT_FEATURE,
                name=specifier,
                response_time=total_time,
                exception=err,
                response_length=0,
            )
            return -1

        total_time = int((time.time() - start_time) * MS_IN_SECOND)
        events.request.fire(
            request_type=EXTRACT_FEATURE,
            name=specifier,
            response_time=total_time,
            exception=None,
            response_length=self._get_result_len(result),
        )
        return result

    def get_100_features(self, user_ids: List[str], specifier):
        start_time = time.time()
        failure = True
        err = ""
        try:
            # Pick a random category
            catogories = [
                random.choice(CATEGORIES) for _ in range(len(user_ids))
            ]
            result = self.client.extract_features(
                output_feature_list=[UserCopyFeatures],
                input_feature_list=[Request],
                input_dataframe=pd.DataFrame(
                    {
                        "Request.user_id": user_ids,
                        "Request.category": catogories,
                    }
                ),
            )
            if result.shape == (len(user_ids), 3):
                failure = False
            else:
                err = "Result shape is not {}x3, it is {}".format(
                    len(user_ids), result.shape
                )
        except Exception as e:
            err = str(e)
            failure = True

        if failure:
            total_time = self._get_elapsed_time(start_time)
            events.request.fire(
                request_type="EXTRACT_100_FEATURES",
                name=specifier,
                response_time=total_time,
                exception=err,
                response_length=0,
            )
            return -1

        total_time = int((time.time() - start_time) * MS_IN_SECOND)
        events.request.fire(
            request_type="EXTRACT_100_FEATURES",
            name=specifier,
            response_time=total_time,
            exception=None,
            response_length=self._get_result_len(result),
        )
        return result


class FennelLocustUser(User):
    abstract = True

    def __init__(self, *args, **kwargs):
        super(FennelLocustUser, self).__init__(*args, **kwargs)
        self.client = FennelLocusClient(self.host)
