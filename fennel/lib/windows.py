import datetime

WindowLen = datetime.timedelta

SECOND = datetime.timedelta(seconds=1)
MINUTE = datetime.timedelta(minutes=1)
HOUR = datetime.timedelta(hours=1)
DAY = datetime.timedelta(days=1)
WEEK = datetime.timedelta(weeks=1)
NOW = datetime.timedelta(0)
FOREVER = datetime.timedelta.max
