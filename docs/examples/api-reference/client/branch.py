from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    def unused_init_branch():
        # docsnip init_branch
        client.init_branch("mybranch")  # docsnip-highlight

        # init checks out client to the new branch
        # so this commit (or any other operations) will be on `mybranch`
        client.commit(...)
        # /docsnip

    def unused_clone_branch():
        # docsnip clone_branch
        client.init_branch("base")
        # do some operations on `base` branch
        client.commit(...)

        # clone `base` branch to `mybranch`
        client.clone_branch("mybranch", "base")  # docsnip-highlight

        # clone checks out client to the new branch
        # so this commit (or any other operations) will be on `mybranch`
        client.commit(...)
        # /docsnip

    def unused_delete_branch():
        # docsnip delete_branch
        client.init_branch("mybranch")

        # do some operations on the branch
        client.commit(...)

        # delete the branch
        client.init_branch("mybranch")  # docsnip-highlight

        # client now points to the main branch
        client.commit(...)
        # /docsnip

    def unused_checkout():
        # docsnip checkout
        # change active branch from `main` to `mybranch`
        # docsnip-highlight start
        client.checkout("mybranch")
        assert client.branch() == "mybranch"
        # docsnip-highlight end

        # all subsequent operations will be on `mybranch`
        client.commit(...)
        # /docsnip
