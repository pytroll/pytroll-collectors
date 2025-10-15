"""Handling the yaml configurations."""

import yaml


def read_config(config_filepath):
    """Read and extract config information."""
    with open(config_filepath, 'r') as fp_:
        config = yaml.safe_load(fp_)

    return config
