import os
import logging
import logging.handlers
from config.app_config import AppConfig


def setup_logging():
    """
    Configure logging for the entire application

    Sets up:
    - Console handler (stdout)
    - File handler (rotating, 10MB max, 5 backups)
    - Consistent formatting
    - Configurable log level
    """
    # Create a logs directory if it doesn't exist
    log_dir = os.path.dirname(AppConfig.LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(AppConfig.get_log_level())

    # Clear any existing handlers
    root_logger.handlers = []

    # Create formatters
    formatter = logging.Formatter(
        AppConfig.LOG_FORMAT,
        datefmt=AppConfig.LOG_DATE_FORMAT
    )

    # Console handler (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(AppConfig.get_log_level())
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (rotating, 10MB max, 5 backups)
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            AppConfig.LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(AppConfig.get_log_level())
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        root_logger.warning(f"Could not create file handler: {e}")

    # Log startup message
    root_logger.info("=" * 80)
    root_logger.info("Edge Service Starting")
    root_logger.info(f"Log Level: {AppConfig.LOG_LEVEL}")
    root_logger.info(f"Log File: {AppConfig.LOG_FILE}")
    root_logger.info("=" * 80)