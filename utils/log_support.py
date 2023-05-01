import logging
import json
import time
import os
from datetime import date


class JSONFormatter(logging.Formatter):
    """JSON formatter"""

    def format(self, record):
        """Format event info to json."""
        string_formatted_time = time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)
        )
        today_date = date.today()
        obj = {}
        obj["message"] = record.msg
        obj["date"] = f"{today_date}"
        obj["level"] = record.levelname
        obj["time"] = f"{string_formatted_time}.{record.msecs:3.0f}Z"
        obj["epoch_time"] = record.created
        if hasattr(record, "custom_logging"):
            for key, value in record.custom_logging.items():
                obj[key] = value
        return json.dumps(obj)


def setup_logger():
    """Create logging object."""
    logger = logging.getLogger(__name__)
    # logger.propagate = False
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.terminator = "\n"
        formatter = JSONFormatter()
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        if "DEBUG" in os.environ and os.environ["DEBUG"] == "true":
            logger.setLevel(logging.DEBUG)  # pragma: no cover
        else:
            logger.setLevel(logging.INFO)

    return logger
