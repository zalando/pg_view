import logging

logger = None


def setup_loggers(options):
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel((logging.INFO if options.verbose else logging.ERROR))
    log_stderr = logging.StreamHandler()
    logger.addHandler(log_stderr)
    return log_stderr
