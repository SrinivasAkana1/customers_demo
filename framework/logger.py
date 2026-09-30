"""Central logging configuration for the bronze pipeline."""

import logging


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create a logger with a consistent format."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
