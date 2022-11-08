from datetime import timedelta
from decimal import Decimal

Duration = str


# Convert a string like "1d 2H 3M 4S" to a timedelta
# Examples
# | Signifier   | Meaning                           |
# | ----------- | --------------------------------- |
# | "10h"       | 10 hours                          |
# | "1w 2m"     | 1 week and 2 minutes              |
# | "1h 10m 2s" | 1 hour, 10 minutes, and 2 seconds |

def duration_to_timedelta(duration_string: Duration) -> timedelta:
    if type(duration_string) != str:
        raise TypeError(
            f"duration {duration_string} must be a specified as a string for "
            f"eg. 1d/2m/3y.")

    total_seconds = Decimal('0')
    prev_num = []
    for character in duration_string:
        if character.isalpha():
            if prev_num:
                num = Decimal(''.join(prev_num))
                if character == 'y':
                    total_seconds += num * 365 * 24 * 60 * 60
                elif character == 'w':
                    total_seconds += num * 7 * 24 * 60 * 60
                elif character == 'm':
                    total_seconds += num * 30 * 24 * 60 * 60
                elif character == 'd':
                    total_seconds += num * 60 * 60 * 24
                elif character == 'H':
                    total_seconds += num * 60 * 60
                elif character == 'M':
                    total_seconds += num * 60
                elif character == 'S':
                    total_seconds += num
                prev_num = []
        elif character.isnumeric() or character == '.':
            prev_num.append(character)
    return timedelta(seconds=float(total_seconds))


def timedelta_to_micros(td: timedelta) -> int:
    return int(td.total_seconds() * 1000000)


def duration_to_micros(duration_string: Duration) -> int:
    return timedelta_to_micros(duration_to_timedelta(duration_string))
