"""S3stalker daemon/runner.

This is a daemon supposed to stay up and running "forever". It will regularly
fetch fresh filenames from an S3 object store and publish the urls of those new
filenames. It is a daemon version of the s3stalker.py script which needs to be
run as a cronjob.

"""

import logging
import logging.config
import sys

from pytroll_collectors.s3stalker import get_configs_from_command_line
from pytroll_collectors.s3stalker_daemon_runner import S3StalkerRunner

logger = logging.getLogger(__name__)


def main():
    """Stalk an s3 bucket."""
    bucket, config, log_config = get_configs_from_command_line()

    if log_config:
        logging.config.dictConfig(log_config)

    logger.info("Try start the s3-stalker runner:")
    try:
        s3runner = S3StalkerRunner(bucket, config)
        s3runner.start()
        s3runner.join()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    except Exception as err:
        logger.error('The S3 Stalker Runner crashed: %s', str(err))
        sys.exit(1)
    finally:
        s3runner.close()


if __name__ == '__main__':
    main()
