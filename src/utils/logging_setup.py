import logging
import sys
from config import settings


def setup_logging():
    """
    Configures the root logger.
    For local development, it uses a simple text format.
    In a container/cloud environment, it's recommended to set LOG_FORMAT=json
    for structured, machine-readable logs.
    """
    if settings.LOG_FORMAT == "json":
        log_formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    else:
        log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(log_formatter)

    logging.basicConfig(level=logging.INFO, handlers=[handler])
