from typing import Optional, List


class Secret:
    arn: str
    role_arn: Optional[str]
    path: List[str]

    def __init__(self, arn, role_arn=None, path=None):
        self.arn = arn
        self.role_arn = role_arn
        self.path = path

    def __getitem__(self, key):
        if self.path is None:
            self.path = []
        return Secret(
            arn=self.arn, role_arn=self.role_arn, path=self.path + [key]
        )
