# utils/logging_utils.py

import logging

def setup_logger(name=__name__):
    """Создание и конфигурация логгера."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(name)
