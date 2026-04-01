#!/usr/bin/env python3
"""
Logging setup for cribl-pusher.

All modules share the single named logger "cribl".
Call setup_logging() once at startup (in main) to attach handlers.
Use get_logger() anywhere else to retrieve the same logger instance.
"""
import logging
import sys

LOGGER_NAME = "cribl"
LOG_FORMAT   = "%(asctime)s  %(levelname)-8s  %(message)s"
DATE_FORMAT  = "%Y-%m-%d %H:%M:%S"

VALID_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def setup_logging(level: str = "INFO", log_file: str = "") -> logging.Logger:
    """
    Configure the 'cribl' logger.

    Args:
        level:    One of DEBUG / INFO / WARNING / ERROR  (case-insensitive).
        log_file: Optional path. If given, logs are written to both console
                  and the file (useful for audit trails).

    Returns the configured logger.
    """
    level_upper = level.upper()
    if level_upper not in VALID_LEVELS:
        level_upper = "INFO"

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, level_upper))

    # Avoid adding duplicate handlers on repeated calls (e.g. in tests)
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    # Console handler — stdout so output can be piped/redirected cleanly
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Optional file handler
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # Do not bubble up to the root logger
    logger.propagate = False

    return logger


def get_logger() -> logging.Logger:
    """Return the shared 'cribl' logger (call setup_logging first)."""
    return logging.getLogger(LOGGER_NAME)
