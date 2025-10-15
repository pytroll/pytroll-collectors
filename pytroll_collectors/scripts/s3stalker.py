"""S3 stalker.

This script fetches filenames newer than a given timedelta and publishes the url
of the corresponding file. It exits after that.
So for now, this script is meant to be run at regular intervals, for example
with a cronjob.
"""

import logging.config

from pytroll_collectors.s3stalker import publish_new_files, get_configs_from_command_line


def main():
    """Stalk an s3 bucket."""
    bucket, config, log_config = get_configs_from_command_line()

    if log_config:
        logging.config.dictConfig(log_config)

    try:
        publish_new_files(bucket, config)
    except KeyboardInterrupt:
        print("terminating publisher...")


if __name__ == '__main__':
    main()
