"""Project-wide logger configuration.

Replaces ad-hoc print() statements with the standard `logging` module so
that log levels, formatting, and (optional) file output are consistent
across all pipeline stages.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, log_file: Path | None = None) -> logging.Logger:
    """Return a configured logger.

    Parameters
    ----------
    name : str
        Logger name (typically ``__name__`` of the calling module).
    log_file : Path, optional
        If supplied, log records are also written to this file.
    """
    logger = logging.getLogger(name)

    # Avoid attaching duplicate handlers if the logger is requested twice
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger
