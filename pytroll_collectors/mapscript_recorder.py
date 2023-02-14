"""A recorder that generates a file for mapscript."""
import json
import yaml
import argparse

from trollsift import compose
from posttroll.message import datetime_encoder


def record_for_mapscript(message_generator, configuration):
    """Record messages to a file for mapscript."""
    filename = configuration["filename"]
    for msg in message_generator():
        with open(filename, mode="a") as fd:
            json_dump = create_json_fields(configuration, msg)
            fd.write(json_dump + "\n")


def create_json_fields(configuration, msg):
    """Create json fields."""
    msg_data = msg.data.copy()
    for item, changes in configuration["aliases"].items():
        msg_data[item] = changes.get(msg_data[item], msg_data[item])
    data = {}
    for field, fmt in configuration["fields"].items():
        try:
            data[field] = msg_data[fmt]
        except KeyError:
            data[field] = compose(fmt, msg_data)
    json_dump = json.dumps(data, default=datetime_encoder)
    return json_dump


def record_command(args=None, message_generator=None):
    """Interface for the command line."""
    config_file = parse_args(args)
    with open(config_file) as fd:
        configuration = yaml.safe_load(fd.read())
    record_for_mapscript(message_generator, configuration)


def parse_args(args):
    """Parse the arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="config file to be used")
    return parser.parse_args(args).config
