from __future__ import annotations

import logging
from logging.handlers import WatchedFileHandler
from pathlib import Path


AUTHFAIL_LOG_PATH = Path("/var/log/pycluster/authfail.log")
_AUTHFAIL_LOGGER_NAME = "pycluster.authfail"


def _authfail_logger() -> logging.Logger:
    logger = logging.getLogger(_AUTHFAIL_LOGGER_NAME)
    if logger.handlers:
        return logger
    try:
        AUTHFAIL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        handler = WatchedFileHandler(AUTHFAIL_LOG_PATH)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        logger.propagate = False
    except Exception:
        return logger
    return logger


def log_auth_failure(app_logger: logging.Logger, channel: str, ip: str, call: str, reason: str) -> None:
    msg = f"AUTHFAIL channel={channel} ip={ip} call={call} reason={reason}"
    app_logger.warning(msg)
    try:
        auth_logger = _authfail_logger()
        if auth_logger.handlers:
            auth_logger.warning(msg)
    except Exception:
        pass
