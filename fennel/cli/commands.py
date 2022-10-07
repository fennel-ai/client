# Copyright 2022 Fennel.ai Authors

####################################################################################################
# Current the follow cli commands are supported:
#  - init
#  - test
#  - view_sources
#  - view_features
#  - view_aggregates
####################################################################################################
import arrow
import os
import click


def greet():
    """Greet a location."""
    tz = "Africa/Addis_Ababa"
    now = arrow.now(tz)
    friendly_time = now.format("h:mm a")
    location = tz.split("/")[-1].replace("_", " ")
    print(f"Hello, {location}! The time is {friendly_time}.")
    return f"Hello, {location}! The time is {friendly_time}."
