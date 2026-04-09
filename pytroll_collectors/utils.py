"""Utility functions."""

import datetime as dt

from posttroll.publisher import create_publisher_from_dict_config


def ensure_utc_aware(value):
    """Return a timezone-aware datetime in UTC.

    Naive datetimes are assumed to already represent UTC.
    """
    if value is None:
        return None
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def check_nameserver_options(nameservers, for_listener=False):
    """Check the nameserver options given by user."""
    if for_listener:
        if isinstance(nameservers, (tuple, list)):
            nameservers = nameservers[0]

    if nameservers is False:
        return nameservers

    if nameservers is not None:
        if isinstance(nameservers, str):
            nameservers = False if nameservers == 'false' else nameservers
        else:
            nameservers = False if 'false' in nameservers or False in nameservers else nameservers

    return nameservers


def create_started_publisher_from_config(publisher_config):
    """Create a started publisher from a dictionary of configuration items."""
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    return publisher


def create_publisher_config_dict(name, nameservers, port):
    """Create pubisher configuration dictionary from the given name, port and nameserver."""
    return {
        'name': name,
        'port': port,
        'nameservers': nameservers,
    }


def fix_start_end_time(mda):
    """Make start and end time coherent."""
    if "duration" in mda and "end_time" not in mda:
        mda["end_time"] = (
            mda["start_time"] + dt.timedelta(seconds=int(mda["duration"]))
        )

    if "start_date" in mda:
        mda["start_time"] = dt.datetime.combine(
            mda["start_date"].date(),
            mda["start_time"].time(),
            tzinfo=dt.timezone.utc,
        )
        if "end_date" not in mda:
            mda["end_date"] = mda["start_date"]
        del mda["start_date"]

    if "end_date" in mda:
        mda["end_time"] = dt.datetime.combine(
            mda["end_date"].date(),
            mda["end_time"].time(),
            tzinfo=dt.timezone.utc,
        )
        del mda["end_date"]

    if "start_time" in mda:
        mda["start_time"] = ensure_utc_aware(mda["start_time"])
    if "end_time" in mda:
        mda["end_time"] = ensure_utc_aware(mda["end_time"])

    if "end_time" in mda:
        while mda["start_time"] > mda["end_time"]:
            mda["end_time"] += dt.timedelta(days=1)

    if "duration" in mda:
        del mda["duration"]

    return mda
