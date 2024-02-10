import logging.config

# Define your logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
        # Add file handler, etc. if needed
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True,
        },
        "bloom_lims.bdb": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False,
        },
        "bloom_lims.bdb.BLOOMdb3": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}


def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
