"""Logging helpers for pytroll-collectors."""

import logging
import logging.config
import logging.handlers
from pathlib import Path

import yaml


DEFAULT_FORMAT = "[%(levelname)s: %(asctime)s : %(name)s] %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _setup_legacy_logging(opts, name):
    """Set up logging using the existing pytroll-collectors behaviour."""
    handlers = []

    if opts.log_config:
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(
                opts.log_config,
                "midnight",
                backupCount=7,
            )
        )

    handlers.append(logging.StreamHandler())

    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    root_logger = logging.getLogger("")
    root_logger.setLevel(loglevel)

    # Avoid duplicating handlers if setup_logging is called more than once
    if root_logger.handlers:
        root_logger.handlers.clear()

    formatter = logging.Formatter(DEFAULT_FORMAT, DEFAULT_DATEFMT)

    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(loglevel)
        root_logger.addHandler(handler)

    # Preserve existing behaviour
    logging.getLogger("posttroll").setLevel(logging.INFO)

    return logging.getLogger(name)


def _setup_logging_from_config(config_path, name):
    """Set up logging from config file.

    YAML files are loaded with dictConfig.
    INI-style files are loaded with fileConfig.
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Logging config file not found: {config_path}")

    suffix = config_path.suffix.lower()

    if suffix in (".yaml", ".yml"):
        with config_path.open("r", encoding="utf-8") as fh:
            config = yaml.safe_load(fh)

        if not isinstance(config, dict):
            raise ValueError(f"Invalid YAML logging config in {config_path}")

        logging.config.dictConfig(config)
    else:
        logging.config.fileConfig(
            str(config_path),
            disable_existing_loggers=False,
        )

    return logging.getLogger(name)


def setup_logging(opts, name):
    """Set up logging.

    Preferred:
      - opts.log_config

    Backward-compatible:
      - opts.stalker_log_config

    Fallback:
      - existing log/verbose-based setup
    """
    log_config = getattr(opts, "log_config", None)
    if not log_config:
        log_config = getattr(opts, "stalker_log_config", None)

    if log_config:
        logger = _setup_logging_from_config(log_config, name)

        # Preserve current collector behaviour unless explicitly overridden
        # by the provided config.
        posttroll_logger = logging.getLogger("posttroll")
        if posttroll_logger.level == logging.NOTSET:
            posttroll_logger.setLevel(logging.INFO)

        return logger

    return _setup_legacy_logging(opts, name)
