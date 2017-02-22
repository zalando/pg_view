import logging

logger = logging.getLogger(__name__)
_log_stderr = logging.StreamHandler()


def enable_logging_to_stderr():
    logger.addHandler(_log_stderr)


def disable_logging_to_stderr():
    logger.removeHandler(_log_stderr)
