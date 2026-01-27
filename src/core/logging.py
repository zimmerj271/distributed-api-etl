import logging
import sys


def configure_logging() -> None:
    """
    Logs emitted from background threads do not appear in Databricks notebook output
    or on the driver log output. To properly redirect logs to be visible in the driver
    output, this function assigns a StreamHandler to direct the output to STDOUT.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove default handlers to avoid duplication
    for h in list(logger.handlers):
        logger.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)  # stream logs to STDOUT for background processes to be visible on the driver 
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))

    logger.addHandler(handler)

def set_py4j_logging_level() -> None:
    """Lowers logging level for py4j.clientserver to avoid unnecessary logging events in STDIO"""
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

