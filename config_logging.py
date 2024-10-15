import logging
from logging.handlers import RotatingFileHandler
import json
import sys


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


def setup_logger(name):
    logger = logging.getLogger(name)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(JsonFormatter())
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(f"{name}.log", maxBytes=2000, backupCount=5)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG)
    return logger
