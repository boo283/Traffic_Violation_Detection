import logging
import logging.config
import yaml
import os

# Load logging configuration
config_folder_path = r"F:\\BigData\\Traffic_Violation_Detection\\config"
config_file_path = os.path.join(config_folder_path, "logging_config.yml")
# Kiểm tra sự tồn tại của tệp cấu hình
if not os.path.exists(config_file_path):
    raise FileNotFoundError(f"Config file not found at {config_file_path}")

with open(config_file_path, 'r') as file:
    config = yaml.safe_load(file)
    logging.config.dictConfig(config)

# Create logger
logger = logging.getLogger('my_module')

# Example log messages
logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")