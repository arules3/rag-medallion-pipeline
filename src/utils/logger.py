import logging


def get_logger(name : str) -> logging.Logger:
    """Configures and returns a standardized logger."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger(name)